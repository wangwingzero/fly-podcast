use std::path::{Path, PathBuf};

use eframe::egui::{self, Color32, RichText, ScrollArea};
use serde::{Deserialize, Serialize};

use crate::pipeline::{Pipeline, StepStatus, STEPS};
use crate::runner::{self, LogLine, RunHandle};
use crate::settings::{FieldType, Settings, SETTING_GROUPS};
use crate::widgets::timeline;

/// Persisted recent directory paths (saved independently).
#[derive(Default, Serialize, Deserialize)]
struct RecentPaths {
    #[serde(skip_serializing_if = "Option::is_none")]
    last_pdf_dir: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_output_dir: Option<PathBuf>,
}

impl RecentPaths {
    fn config_path(project_root: &Path) -> PathBuf {
        project_root.join("podcast-studio.json")
    }

    fn load(project_root: &Path) -> Self {
        let path = Self::config_path(project_root);
        std::fs::read_to_string(&path)
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default()
    }

    fn save(&self, project_root: &Path) {
        let path = Self::config_path(project_root);
        if let Ok(json) = serde_json::to_string_pretty(self) {
            let _ = std::fs::write(path, json);
        }
    }
}

/// Which page is currently shown.
#[derive(PartialEq)]
enum Page {
    Pipeline,
    Settings,
}

/// Main application state.
pub struct PodcastApp {
    page: Page,
    pipeline: Pipeline,
    log_lines: Vec<LogLine>,
    run_handle: Option<RunHandle>,
    script_content: String,
    script_dirty: bool,
    settings: Settings,
    settings_status: String,
    /// Last directory used for PDF file picker.
    last_pdf_dir: Option<PathBuf>,
    /// Last directory used for output folder picker.
    last_output_dir: Option<PathBuf>,
    /// Project root for saving recent paths.
    project_root: PathBuf,
}

impl PodcastApp {
    pub fn new(cc: &eframe::CreationContext<'_>) -> Self {
        // Load system Chinese font for CJK character support
        Self::setup_fonts(&cc.egui_ctx);

        // Find project root (parent of podcast-studio/)
        let project_root = find_project_root();
        let settings = Settings::load(&project_root);
        let recent = RecentPaths::load(&project_root);

        Self {
            page: Page::Pipeline,
            pipeline: Pipeline::new(),
            log_lines: Vec::new(),
            run_handle: None,
            script_content: String::new(),
            script_dirty: false,
            settings,
            settings_status: String::new(),
            last_pdf_dir: recent.last_pdf_dir,
            last_output_dir: recent.last_output_dir,
            project_root,
        }
    }

    fn setup_fonts(ctx: &egui::Context) {
        let mut fonts = egui::FontDefinitions::default();

        // Try common Chinese font paths on Windows
        let font_paths = [
            "C:/Windows/Fonts/msyh.ttc",    // Microsoft YaHei
            "C:/Windows/Fonts/simhei.ttf",   // SimHei
            "C:/Windows/Fonts/simsun.ttc",   // SimSun
        ];

        let mut loaded = false;
        for path in &font_paths {
            if let Ok(font_data) = std::fs::read(path) {
                fonts.font_data.insert(
                    "chinese".to_owned(),
                    egui::FontData::from_owned(font_data).into(),
                );

                // Insert Chinese font as fallback for proportional and monospace
                if let Some(family) = fonts.families.get_mut(&egui::FontFamily::Proportional) {
                    family.push("chinese".to_owned());
                }
                if let Some(family) = fonts.families.get_mut(&egui::FontFamily::Monospace) {
                    family.push("chinese".to_owned());
                }

                loaded = true;
                break;
            }
        }

        if !loaded {
            eprintln!("Warning: no Chinese font found, CJK characters may not display correctly");
        }

        ctx.set_fonts(fonts);
    }

    /// Save recent PDF/output directory paths to disk.
    fn save_recent_paths(&self) {
        let recent = RecentPaths {
            last_pdf_dir: self.last_pdf_dir.clone(),
            last_output_dir: self.last_output_dir.clone(),
        };
        recent.save(&self.project_root);
    }

    /// Poll the running subprocess for new log output.
    fn poll_subprocess(&mut self) {
        if let Some(handle) = &mut self.run_handle {
            // Drain available log lines
            while let Ok(line) = handle.rx.try_recv() {
                self.log_lines.push(line);
            }

            // Check if process finished
            if let Some(status) = handle.try_finish() {
                if status.success() {
                    // Determine what to do based on current step
                    match self.pipeline.current_step {
                        1 => {
                            // Script generation done — extract work_dir from logs
                            self.extract_work_dir_from_logs();
                            self.pipeline.advance();
                            self.load_script();
                        }
                        3 => {
                            // Audio generation done
                            self.pipeline.advance();
                        }
                        4 => {
                            // Publish done
                            self.pipeline.complete_current();
                        }
                        _ => {
                            self.pipeline.advance();
                        }
                    }
                } else {
                    let code = status.code().unwrap_or(-1);
                    self.pipeline.fail(format!("Process exited with code {code}"));
                }
                self.run_handle = None;
            }
        }
    }

    /// Try to extract work_dir path from log output.
    fn extract_work_dir_from_logs(&mut self) {
        for line in self.log_lines.iter().rev() {
            if line.text.contains("Output dir:") {
                if let Some(path_str) = line.text.split("Output dir:").nth(1) {
                    let path = PathBuf::from(path_str.trim());
                    if path.exists() {
                        self.pipeline.work_dir = Some(path);
                        return;
                    }
                }
            }
            if line.text.contains("Script generation complete:") {
                if let Some(path_str) = line.text.split("Script generation complete:").nth(1) {
                    let path = PathBuf::from(path_str.trim());
                    if path.exists() {
                        self.pipeline.work_dir = Some(path);
                        return;
                    }
                }
            }
        }

        // Fallback: if we have pdf_path, construct expected work_dir
        if let Some(pdf) = &self.pipeline.pdf_path {
            let stem = pdf.file_stem().unwrap_or_default().to_string_lossy();
            let today = chrono_today();
            // Look for the directory
            let expected = PathBuf::from("data/output/podcast").join(format!("{today}_{stem}"));
            if expected.exists() {
                self.pipeline.work_dir = Some(expected);
            }
        }
    }

    /// Load script.json content for editing.
    fn load_script(&mut self) {
        if let Some(dir) = &self.pipeline.work_dir {
            let script_path = dir.join("script.json");
            if script_path.exists() {
                match std::fs::read_to_string(&script_path) {
                    Ok(content) => {
                        self.script_content = content;
                        self.script_dirty = false;
                    }
                    Err(e) => {
                        self.script_content = format!("Error reading script.json: {e}");
                    }
                }
            }
        }
    }

    /// Jump to any step. If jumping forward to step 2+, prompt for work_dir if missing.
    fn jump_to_step(&mut self, target: usize) {
        if target == self.pipeline.current_step {
            return;
        }

        // Steps 2-4 need work_dir
        if target >= 2 && self.pipeline.work_dir.is_none() {
            // Ask user to select the script.json file directly
            if let Some(file) = rfd::FileDialog::new()
                .set_title("选择剧本文件 (script.json)")
                .add_filter("JSON", &["json"])
                .pick_file()
            {
                if let Some(dir) = file.parent() {
                    self.pipeline.work_dir = Some(dir.to_path_buf());
                } else {
                    return;
                }
            } else {
                return;
            }
        }

        // Mark skipped steps as Done
        for i in 0..target {
            if self.pipeline.steps[i] == StepStatus::Pending {
                self.pipeline.steps[i] = StepStatus::Done;
            }
        }

        // Reset target step to Pending so user can act on it
        self.pipeline.steps[target] = StepStatus::Pending;
        self.pipeline.current_step = target;

        // Load script if jumping to edit step
        if target == 2 {
            self.load_script();
        }
    }

    /// Save script.json back to disk.
    fn save_script(&mut self) {
        if let Some(dir) = &self.pipeline.work_dir {
            let script_path = dir.join("script.json");
            match std::fs::write(&script_path, &self.script_content) {
                Ok(()) => {
                    self.script_dirty = false;
                }
                Err(e) => {
                    self.log_lines.push(LogLine {
                        text: format!("Failed to save script.json: {e}"),
                        is_stderr: true,
                    });
                }
            }
        }
    }

    /// Draw the right panel content for the current step.
    fn draw_step_content(&mut self, ui: &mut egui::Ui) {
        let step = self.pipeline.current_step;

        ui.add_space(8.0);

        match step {
            0 => self.draw_step_select_pdf(ui),
            1 => self.draw_step_generate_script(ui),
            2 => self.draw_step_edit_script(ui),
            3 => self.draw_step_generate_audio(ui),
            4 => self.draw_step_publish(ui),
            _ => {}
        }
    }

    // ── Step 0: Select PDF ──────────────────────────────────────

    fn draw_step_select_pdf(&mut self, ui: &mut egui::Ui) {
        // PDF selection
        ui.horizontal(|ui| {
            ui.label("PDF 文件:");
            if let Some(path) = &self.pipeline.pdf_path {
                ui.monospace(path.display().to_string());
            } else {
                ui.colored_label(Color32::from_rgb(156, 163, 175), "未选择");
            }
        });
        if ui.button("选择 PDF 文件...").clicked() {
            let mut dialog = rfd::FileDialog::new()
                .add_filter("PDF", &["pdf"]);
            if let Some(dir) = &self.last_pdf_dir {
                dialog = dialog.set_directory(dir);
            }
            if let Some(path) = dialog.pick_file() {
                if let Some(parent) = path.parent() {
                    self.last_pdf_dir = Some(parent.to_path_buf());
                }
                self.pipeline.pdf_path = Some(path);
                self.save_recent_paths();
            }
        }

        ui.add_space(12.0);

        // Output directory selection
        ui.horizontal(|ui| {
            ui.label("保存位置:");
            if let Some(dir) = &self.pipeline.output_dir {
                ui.monospace(dir.display().to_string());
            } else {
                ui.colored_label(Color32::from_rgb(156, 163, 175), "未选择");
            }
        });
        if ui.button("选择输出文件夹...").clicked() {
            let mut dialog = rfd::FileDialog::new();
            if let Some(dir) = &self.last_output_dir {
                dialog = dialog.set_directory(dir);
            }
            if let Some(dir) = dialog.pick_folder() {
                self.last_output_dir = Some(dir.clone());
                self.pipeline.output_dir = Some(dir);
                self.save_recent_paths();
            }
        }

        ui.add_space(16.0);

        // Next step (both must be selected)
        let ready = self.pipeline.pdf_path.is_some() && self.pipeline.output_dir.is_some();
        ui.add_enabled_ui(ready, |ui| {
            if ui.button("下一步 →").clicked() {
                self.pipeline.advance();
            }
        });
    }

    // ── Step 1: Generate Script ─────────────────────────────────

    fn draw_step_generate_script(&mut self, ui: &mut egui::Ui) {
        let is_running = self.run_handle.is_some();

        if !is_running && self.pipeline.steps[1] == StepStatus::Pending {
            let pdf_str = self.pipeline.pdf_path.as_ref().map(|p| p.display().to_string());
            let out_str = self.pipeline.output_dir.as_ref().map(|p| p.display().to_string());
            if let (Some(pdf_display), Some(out_display)) = (pdf_str, out_str) {
                ui.label(format!("PDF: {pdf_display}"));
                ui.label(format!("输出: {out_display}"));
                ui.add_space(8.0);

                if ui.button("开始生成剧本").clicked() {
                    self.log_lines.clear();
                    self.pipeline.set_running();
                    self.run_handle = Some(runner::spawn_python(&[
                        "podcast-script", "--pdf", &pdf_display,
                        "--output-dir", &out_display,
                    ]));
                }
            } else {
                ui.label("请先选择 PDF 文件和输出文件夹。");
            }
        }

        // Show failed state with retry
        if let StepStatus::Failed(ref msg) = self.pipeline.steps[1] {
            ui.colored_label(Color32::from_rgb(239, 68, 68), format!("失败: {msg}"));
            if ui.button("重试").clicked() {
                self.pipeline.steps[1] = StepStatus::Pending;
            }
        }

        self.draw_log_panel(ui);
    }

    // ── Step 2: Edit Script ─────────────────────────────────────

    fn draw_step_edit_script(&mut self, ui: &mut egui::Ui) {
        if let Some(dir) = self.pipeline.work_dir.clone() {
            let script_path = dir.join("script.json");

            ui.horizontal(|ui| {
                if ui.button("在 VS Code 中打开").clicked() {
                    runner::open_in_vscode(&script_path);
                }
                if ui.button("用默认编辑器打开").clicked() {
                    runner::open_in_editor(&script_path);
                }
                if ui.button("重新加载").clicked() {
                    self.load_script();
                }
                if self.script_dirty {
                    if ui.button("保存").clicked() {
                        self.save_script();
                    }
                    ui.colored_label(Color32::from_rgb(234, 179, 8), "(未保存)");
                }
            });

            ui.add_space(8.0);

            // Inline editor
            ScrollArea::vertical()
                .max_height(ui.available_height() - 50.0)
                .show(ui, |ui| {
                    let response = ui.add(
                        egui::TextEdit::multiline(&mut self.script_content)
                            .code_editor()
                            .desired_width(f32::INFINITY),
                    );
                    if response.changed() {
                        self.script_dirty = true;
                    }
                });

            ui.add_space(8.0);
            ui.horizontal(|ui| {
                if ui.button("← 重新生成剧本").clicked() {
                    self.pipeline.current_step = 1;
                    self.pipeline.steps[1] = StepStatus::Pending;
                    self.pipeline.steps[2] = StepStatus::Pending;
                }
                let next_label = if self.script_dirty { "保存并继续 →" } else { "下一步 →" };
                if ui.button(next_label).clicked() {
                    if self.script_dirty {
                        self.save_script();
                    }
                    self.pipeline.advance();
                }
            });
        } else {
            ui.label("工作目录未找到，请返回重新生成剧本。");
        }
    }

    // ── Step 3: Generate Audio ──────────────────────────────────

    fn draw_step_generate_audio(&mut self, ui: &mut egui::Ui) {
        let is_running = self.run_handle.is_some();

        if !is_running && self.pipeline.steps[3] == StepStatus::Pending {
            let dir_str = self.pipeline.work_dir.as_ref().map(|d| d.display().to_string());
            if let Some(dir_display) = dir_str {
                ui.label(format!("工作目录: {dir_display}"));
                ui.add_space(8.0);

                if ui.button("开始合成音频").clicked() {
                    self.log_lines.clear();
                    self.pipeline.set_running();
                    self.run_handle = Some(runner::spawn_python(&[
                        "podcast-audio", "--dir", &dir_display,
                    ]));
                }
            }
        }

        if let StepStatus::Failed(ref msg) = self.pipeline.steps[3] {
            ui.colored_label(Color32::from_rgb(239, 68, 68), format!("失败: {msg}"));
            if ui.button("重试").clicked() {
                self.pipeline.steps[3] = StepStatus::Pending;
            }
        }

        self.draw_log_panel(ui);
    }

    // ── Step 4: Publish ─────────────────────────────────────────

    fn draw_step_publish(&mut self, ui: &mut egui::Ui) {
        let is_running = self.run_handle.is_some();

        if self.pipeline.steps[4] == StepStatus::Done {
            ui.colored_label(
                Color32::from_rgb(34, 197, 94),
                "发布完成！草稿已创建。",
            );
        } else if !is_running && self.pipeline.steps[4] == StepStatus::Pending {
            let dir_str = self.pipeline.work_dir.as_ref().map(|d| d.display().to_string());
            if let Some(dir_display) = &dir_str {
                // Show MP3 path if exists
                if let Some(dir) = &self.pipeline.work_dir {
                    let meta_path = dir.join("metadata.json");
                    if meta_path.exists() {
                        if let Ok(content) = std::fs::read_to_string(&meta_path) {
                            if let Ok(meta) = serde_json::from_str::<serde_json::Value>(&content) {
                                if let Some(mp3) = meta.get("mp3_path").and_then(|v| v.as_str()) {
                                    ui.label(format!("MP3: {mp3}"));
                                }
                                if let Some(url) = meta.get("mp3_cdn_url").and_then(|v| v.as_str()) {
                                    ui.label(format!("CDN: {url}"));
                                }
                            }
                        }
                    }
                }

                ui.add_space(8.0);
                if ui.button("上传并创建微信草稿").clicked() {
                    self.log_lines.clear();
                    self.pipeline.set_running();
                    self.run_handle = Some(runner::spawn_python(&[
                        "publish-podcast", "--podcast-dir", dir_display,
                    ]));
                }
            }
        }

        if let StepStatus::Failed(ref msg) = self.pipeline.steps[4] {
            ui.colored_label(Color32::from_rgb(239, 68, 68), format!("失败: {msg}"));
            if ui.button("重试").clicked() {
                self.pipeline.steps[4] = StepStatus::Pending;
            }
        }

        self.draw_log_panel(ui);
    }

    // ── Settings page ─────────────────────────────────────────────

    fn draw_settings_page(&mut self, ui: &mut egui::Ui) {
        ui.heading("设置");
        ui.add_space(4.0);
        ui.label(
            RichText::new(format!("配置文件: {}", self.settings.env_path.display()))
                .color(Color32::from_rgb(156, 163, 175))
                .size(12.0),
        );
        ui.add_space(8.0);

        ScrollArea::vertical().show(ui, |ui| {
            for (group_name, fields) in SETTING_GROUPS {
                ui.add_space(8.0);
                ui.label(RichText::new(*group_name).strong().size(14.0));
                ui.separator();

                egui::Grid::new(*group_name)
                    .num_columns(3)
                    .spacing([8.0, 6.0])
                    .striped(true)
                    .show(ui, |ui| {
                        for field in *fields {
                            ui.label(field.label);

                            match &field.field_type {
                                FieldType::Toggle => {
                                    let mut checked = self.settings.get_bool(field.key);
                                    if ui.checkbox(&mut checked, "").changed() {
                                        self.settings.set_bool(field.key, checked);
                                    }
                                    ui.label(""); // empty column
                                }
                                FieldType::Text { is_secret, placeholder } => {
                                    let mut val = self.settings.get(field.key).to_string();
                                    let is_visible = !is_secret
                                        || self.settings.visible_secrets.contains(field.key);

                                    let response = if is_visible {
                                        ui.add_sized(
                                            [320.0, 20.0],
                                            egui::TextEdit::singleline(&mut val)
                                                .hint_text(*placeholder),
                                        )
                                    } else {
                                        ui.add_sized(
                                            [320.0, 20.0],
                                            egui::TextEdit::singleline(&mut val)
                                                .hint_text(*placeholder)
                                                .password(true),
                                        )
                                    };

                                    if response.changed() {
                                        self.settings.set(field.key, val);
                                    }

                                    if *is_secret {
                                        let icon = if is_visible { "\u{1F441}" } else { "*" };
                                        if ui.small_button(icon).clicked() {
                                            if is_visible {
                                                self.settings.visible_secrets.remove(field.key);
                                            } else {
                                                self.settings.visible_secrets.insert(field.key.to_string());
                                            }
                                        }
                                    } else {
                                        ui.label("");
                                    }
                                }
                            }

                            ui.end_row();
                        }
                    });
            }

            ui.add_space(16.0);

            ui.horizontal(|ui| {
                let save_enabled = self.settings.dirty;
                ui.add_enabled_ui(save_enabled, |ui| {
                    if ui.button("保存").clicked() {
                        match self.settings.save() {
                            Ok(()) => self.settings_status = "已保存".to_string(),
                            Err(e) => self.settings_status = e,
                        }
                    }
                });

                if !self.settings_status.is_empty() {
                    let color = if self.settings_status.starts_with("已") {
                        Color32::from_rgb(34, 197, 94)
                    } else {
                        Color32::from_rgb(239, 68, 68)
                    };
                    ui.colored_label(color, &self.settings_status);
                }

                if self.settings.dirty {
                    ui.colored_label(Color32::from_rgb(234, 179, 8), "(未保存)");
                }
            });
        });
    }

    // ── Log panel (shared by steps 1, 3, 4) ─────────────────────

    fn draw_log_panel(&self, ui: &mut egui::Ui) {
        if self.log_lines.is_empty() {
            return;
        }

        ui.add_space(8.0);
        ui.separator();
        ui.label(RichText::new("输出日志").strong());

        ScrollArea::vertical()
            .max_height(ui.available_height() - 20.0)
            .stick_to_bottom(true)
            .show(ui, |ui| {
                for line in &self.log_lines {
                    let color = if line.is_stderr {
                        Color32::from_rgb(234, 179, 8) // yellow for stderr
                    } else {
                        Color32::from_rgb(209, 213, 219) // light gray
                    };
                    ui.monospace(RichText::new(&line.text).color(color).size(12.0));
                }
            });
    }
}

impl eframe::App for PodcastApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        // Poll subprocess
        self.poll_subprocess();

        // Request repaint while subprocess is running
        if self.run_handle.is_some() {
            ctx.request_repaint();
        }

        // Bottom bar: page navigation
        egui::TopBottomPanel::bottom("nav_bar").show(ctx, |ui| {
            ui.add_space(4.0);
            ui.horizontal(|ui| {
                let pipeline_selected = self.page == Page::Pipeline;
                let settings_selected = self.page == Page::Settings;

                if ui.selectable_label(pipeline_selected, "制作").clicked() {
                    self.page = Page::Pipeline;
                }
                if ui.selectable_label(settings_selected, "设置").clicked() {
                    self.page = Page::Settings;
                }
            });
            ui.add_space(2.0);
        });

        match self.page {
            Page::Pipeline => {
                // Left panel: timeline
                egui::SidePanel::left("timeline_panel")
                    .min_width(180.0)
                    .max_width(220.0)
                    .resizable(false)
                    .show(ctx, |ui| {
                        ui.add_space(8.0);

                        if let Some(clicked) = timeline::draw_timeline(
                            ui,
                            &self.pipeline.steps,
                            self.pipeline.current_step,
                        ) {
                            self.jump_to_step(clicked);
                        }

                        ui.with_layout(egui::Layout::bottom_up(egui::Align::Center), |ui| {
                            ui.add_space(8.0);
                            if ui.small_button("重置").clicked() {
                                self.pipeline.reset();
                                self.log_lines.clear();
                                self.script_content.clear();
                                self.script_dirty = false;
                                self.run_handle = None;
                            }
                            ui.add_space(4.0);
                        });
                    });

                // Central panel: step content
                egui::CentralPanel::default().show(ctx, |ui| {
                    self.draw_step_content(ui);
                });
            }
            Page::Settings => {
                egui::CentralPanel::default().show(ctx, |ui| {
                    self.draw_settings_page(ui);
                });
            }
        }
    }
}

/// Get today's date as YYYY-MM-DD string (no chrono dependency).
fn chrono_today() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    // Beijing time: UTC+8
    let secs = secs + 8 * 3600;
    let days = secs / 86400;
    // Days since 1970-01-01
    let (y, m, d) = days_to_date(days);
    format!("{y:04}-{m:02}-{d:02}")
}

fn days_to_date(days: u64) -> (u64, u64, u64) {
    // Algorithm from http://howardhinnant.github.io/date_algorithms.html
    let z = days + 719468;
    let era = z / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

/// Find project root by walking up from exe dir looking for run.py.
fn find_project_root() -> PathBuf {
    let exe = std::env::current_exe().unwrap_or_default();
    let mut dir = exe.parent().map(|p| p.to_path_buf()).unwrap_or_default();
    for _ in 0..10 {
        if dir.join("run.py").exists() {
            return dir;
        }
        if let Some(parent) = dir.parent() {
            dir = parent.to_path_buf();
        } else {
            break;
        }
    }
    std::env::current_dir().unwrap_or_default()
}
