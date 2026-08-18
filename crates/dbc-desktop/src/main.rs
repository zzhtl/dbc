mod app;
mod atomic_file;
mod drivers;
mod export;
mod file_picker;
mod fonts;
mod labels;
mod paging;
mod palette;
mod result_table;
mod shortcuts;
mod status;
mod store;
mod table_editor;
mod text_format;
mod tasks;
mod vault_prompt;
mod write_guard;

use eframe::egui;

use crate::app::DbcApp;

fn main() -> eframe::Result {
    let native_options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_app_id("dbc")
            .with_inner_size([1440.0, 900.0])
            .with_min_inner_size([960.0, 640.0]),
        renderer: eframe::Renderer::Glow,
        centered: true,
        persist_window: false,
        ..eframe::NativeOptions::default()
    };

    eframe::run_native(
        "DBC",
        native_options,
        Box::new(|creation_context| {
            let font = fonts::install_cjk_font(&creation_context.egui_ctx);
            Ok(Box::new(DbcApp::new(store::Store::open(), font.is_none())?))
        }),
    )
}
