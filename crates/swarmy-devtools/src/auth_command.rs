/// Only interactive login and file import live in the provider helper.
pub(crate) enum Command {
    Login {
        provider: String,
        label: Option<String>,
        resource: Option<String>,
        scope: Option<String>,
    },
    Import {
        file: Option<std::path::PathBuf>,
        label: Option<String>,
    },
}
