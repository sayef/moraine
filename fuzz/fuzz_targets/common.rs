//! Shared by every target (included with `mod common;`).

/// libfuzzer-sys aborts on any panic, even one moraine catches on purpose (its decoders turn
/// panics inside arrow and parquet into errors). A plain hook keeps those caught panics
/// harmless; an uncaught one still ends the run, because libfuzzer-sys aborts when a panic
/// leaves the target.
pub fn init() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        std::panic::set_hook(Box::new(|info| eprintln!("panic: {info}")));
    });
}
