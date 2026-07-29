mod app;
mod drivers;
mod export;
mod result_table;

use eframe::egui;
use egui_system_fonts::FontStyle;

use crate::app::DbcApp;

fn main() -> eframe::Result {
    let native_options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_app_id("dbc")
            .with_inner_size([1440.0, 900.0])
            .with_min_inner_size([960.0, 640.0]),
        renderer: eframe::Renderer::Wgpu,
        centered: true,
        persist_window: false,
        ..eframe::NativeOptions::default()
    };

    eframe::run_native(
        "DBC",
        native_options,
        Box::new(|creation_context| {
            egui_system_fonts::add_auto(&creation_context.egui_ctx, FontStyle::Sans);
            Ok(Box::new(DbcApp::new()?))
        }),
    )
}
