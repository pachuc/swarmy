//! Load a config file with the new grouped Settings; used by script tests.
fn main() {
    let path = std::env::args().nth(1).expect("usage: check-config <file>");
    match swarmy_config::Settings::read(std::path::Path::new(&path)) {
        Ok(settings) => {
            println!(
                "ok provider={} store={}",
                settings.selection.provider, settings.store.directory
            );
        }
        Err(error) => {
            eprintln!("invalid config: {error}");
            std::process::exit(1);
        }
    }
}
