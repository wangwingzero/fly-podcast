mod app;
mod pipeline;
mod runner;
mod settings;
mod widgets;

fn main() -> eframe::Result {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_secs()
        .init();
    let options = eframe::NativeOptions {
        viewport: eframe::egui::ViewportBuilder::default()
            .with_title("飞行播客工作站")
            .with_inner_size([960.0, 640.0])
            .with_min_inner_size([800.0, 500.0]),
        ..Default::default()
    };

    eframe::run_native(
        "podcast-studio",
        options,
        Box::new(|cc| Ok(Box::new(app::PodcastApp::new(cc)))),
    )
}
