use eframe::egui::{self, Color32, Pos2, Rect, Stroke, Vec2};

use crate::pipeline::{StepStatus, STEPS};

const CIRCLE_RADIUS: f32 = 14.0;
const LINE_WIDTH: f32 = 3.0;
const STEP_SPACING: f32 = 90.0;

const COLOR_DONE: Color32 = Color32::from_rgb(34, 197, 94);     // green
const COLOR_RUNNING: Color32 = Color32::from_rgb(59, 130, 246); // blue
const COLOR_FAILED: Color32 = Color32::from_rgb(239, 68, 68);   // red
const COLOR_PENDING: Color32 = Color32::from_rgb(156, 163, 175); // gray
const COLOR_CURRENT_BG: Color32 = Color32::from_rgb(239, 246, 255); // light blue bg

fn status_color(status: &StepStatus) -> Color32 {
    match status {
        StepStatus::Done => COLOR_DONE,
        StepStatus::Running => COLOR_RUNNING,
        StepStatus::Failed(_) => COLOR_FAILED,
        StepStatus::Pending => COLOR_PENDING,
    }
}

fn status_icon(status: &StepStatus) -> &'static str {
    match status {
        StepStatus::Done => "\u{2714}",    // check mark
        StepStatus::Running => "\u{23F3}", // hourglass
        StepStatus::Failed(_) => "\u{2716}", // X mark
        StepStatus::Pending => "",
    }
}

/// Draw the vertical timeline on the left panel. Returns the index of clicked step (if any).
pub fn draw_timeline(
    ui: &mut egui::Ui,
    steps: &[StepStatus; 5],
    current_step: usize,
) -> Option<usize> {
    let start_y = 40.0;
    let left_x = 40.0;
    let panel_rect = ui.available_rect_before_wrap();
    let base_y = panel_rect.min.y; // Y offset from panel top (below heading/separator)

    // Collect label rects for click handling (computed during paint)
    let mut label_rects: [(Rect, bool); 5] = [(Rect::NOTHING, false); 5];

    // Paint everything first
    {
        let painter = ui.painter();

        // Draw connecting lines
        for i in 0..4 {
            let y1 = base_y + start_y + i as f32 * STEP_SPACING + CIRCLE_RADIUS;
            let y2 = base_y + start_y + (i + 1) as f32 * STEP_SPACING - CIRCLE_RADIUS;
            let color = if steps[i] == StepStatus::Done {
                COLOR_DONE
            } else {
                COLOR_PENDING.linear_multiply(0.5)
            };
            painter.line_segment(
                [
                    Pos2::new(panel_rect.min.x + left_x, y1),
                    Pos2::new(panel_rect.min.x + left_x, y2),
                ],
                Stroke::new(LINE_WIDTH, color),
            );
        }

        // Draw circles and labels
        for (i, step_info) in STEPS.iter().enumerate() {
            let center_y = base_y + start_y + i as f32 * STEP_SPACING;
            let center = Pos2::new(panel_rect.min.x + left_x, center_y);
            let color = status_color(&steps[i]);

            // Highlight background for current step
            if i == current_step {
                let highlight_rect = Rect::from_min_size(
                    Pos2::new(panel_rect.min.x + 4.0, center_y - 22.0),
                    Vec2::new(panel_rect.width() - 8.0, 44.0),
                );
                painter.rect_filled(highlight_rect, 6.0, COLOR_CURRENT_BG);
            }

            // Circle
            if steps[i] == StepStatus::Done {
                painter.circle_filled(center, CIRCLE_RADIUS, color);
            } else {
                painter.circle_stroke(center, CIRCLE_RADIUS, Stroke::new(2.5, color));
            }

            // Icon inside circle
            let icon = status_icon(&steps[i]);
            if !icon.is_empty() {
                let icon_color = if steps[i] == StepStatus::Done {
                    Color32::WHITE
                } else {
                    color
                };
                painter.text(
                    center,
                    egui::Align2::CENTER_CENTER,
                    icon,
                    egui::FontId::proportional(12.0),
                    icon_color,
                );
            } else {
                painter.text(
                    center,
                    egui::Align2::CENTER_CENTER,
                    format!("{}", i + 1),
                    egui::FontId::proportional(12.0),
                    color,
                );
            }

            // Step label
            let label_pos = Pos2::new(center.x + CIRCLE_RADIUS + 12.0, center_y);
            let text_color = if i == current_step {
                Color32::from_rgb(30, 58, 138)
            } else {
                Color32::from_rgb(75, 85, 99)
            };

            let label_rect = painter.text(
                label_pos,
                egui::Align2::LEFT_CENTER,
                step_info.name,
                egui::FontId::proportional(14.0),
                text_color,
            );

            // Full row clickable area (circle + label + padding)
            let row_rect = Rect::from_min_size(
                Pos2::new(panel_rect.min.x, center_y - STEP_SPACING / 2.0),
                Vec2::new(panel_rect.width(), STEP_SPACING),
            );
            label_rects[i] = (row_rect, true);
        }
    }
    // painter borrow released here

    // Reserve space first, then handle clicks
    let total_height = start_y + 4.0 * STEP_SPACING + 40.0;
    ui.allocate_space(Vec2::new(panel_rect.width(), total_height));

    // Handle clicks (separate pass, no painter borrow)
    let mut clicked = None;
    for (i, (rect, clickable)) in label_rects.iter().enumerate() {
        if *clickable {
            let response = ui.allocate_rect(*rect, egui::Sense::click());
            if response.clicked() {
                clicked = Some(i);
            }
            // Hover cursor hint
            if response.hovered() {
                ui.ctx().set_cursor_icon(egui::CursorIcon::PointingHand);
            }
        }
    }

    clicked
}
