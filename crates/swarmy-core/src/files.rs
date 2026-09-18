use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadArguments {
    pub path: String,
    #[serde(default = "first_line")]
    pub offset: usize,
    #[serde(default = "read_limit")]
    pub limit: usize,
}
const fn first_line() -> usize {
    1
}
const fn read_limit() -> usize {
    2000
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WriteArguments {
    pub path: String,
    pub content: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EditArguments {
    pub path: String,
    pub old_string: String,
    pub new_string: String,
    #[serde(default)]
    pub replace_all: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearchArguments {
    pub pattern: String,
    #[serde(default = "current_directory")]
    pub path: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LsArguments {
    #[serde(default = "current_directory")]
    pub path: String,
}
fn current_directory() -> String {
    ".".into()
}
