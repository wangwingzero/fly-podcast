use std::path::PathBuf;

/// Status of a single pipeline step.
#[derive(Clone, Debug, PartialEq)]
pub enum StepStatus {
    Pending,
    Running,
    Done,
    Failed(String),
}

impl StepStatus {
    pub fn is_terminal(&self) -> bool {
        matches!(self, StepStatus::Done | StepStatus::Failed(_))
    }
}

/// Human-readable info for each step.
pub struct StepInfo {
    pub name: &'static str,
    pub description: &'static str,
}

pub const STEPS: [StepInfo; 5] = [
    StepInfo { name: "选择 PDF",  description: "选择要转换的 PDF 文件" },
    StepInfo { name: "生成剧本", description: "调用 LLM 生成对话剧本" },
    StepInfo { name: "编辑剧本", description: "查看和编辑 script.json" },
    StepInfo { name: "生成音频", description: "TTS 合成 + 音频拼接" },
    StepInfo { name: "上传发布", description: "上传到 R2 并创建微信草稿" },
];

/// The 5-step podcast pipeline state.
pub struct Pipeline {
    pub pdf_path: Option<PathBuf>,
    pub output_dir: Option<PathBuf>,
    pub work_dir: Option<PathBuf>,
    pub steps: [StepStatus; 5],
    pub current_step: usize,
}

impl Pipeline {
    pub fn new() -> Self {
        Self {
            pdf_path: None,
            output_dir: None,
            work_dir: None,
            steps: [
                StepStatus::Pending,
                StepStatus::Pending,
                StepStatus::Pending,
                StepStatus::Pending,
                StepStatus::Pending,
            ],
            current_step: 0,
        }
    }

    pub fn reset(&mut self) {
        *self = Self::new();
    }

    /// Advance to the next step after completing the current one.
    pub fn advance(&mut self) {
        if self.current_step < 4 {
            self.steps[self.current_step] = StepStatus::Done;
            self.current_step += 1;
        }
    }

    pub fn fail(&mut self, msg: String) {
        self.steps[self.current_step] = StepStatus::Failed(msg);
    }

    pub fn set_running(&mut self) {
        self.steps[self.current_step] = StepStatus::Running;
    }

    pub fn complete_current(&mut self) {
        self.steps[self.current_step] = StepStatus::Done;
    }

    /// Can the user retry the current step?
    pub fn can_retry(&self) -> bool {
        matches!(self.steps[self.current_step], StepStatus::Failed(_))
    }
}
