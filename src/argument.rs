use std::path::PathBuf;

use structopt::clap::AppSettings::{ColorAuto, ColoredHelp};
use structopt::StructOpt;

#[derive(Debug, StructOpt, Clone)]
#[structopt(
name = "image_bed",
about = "An image bed.",
setting(ColorAuto),
setting(ColoredHelp)
)]
pub struct Argument {
    #[structopt(short, long, help = "config path, `-` means read config from stdin")]
    pub config: PathBuf,
}

impl Argument {
    pub fn new() -> Self {
        Argument::from_args()
    }
}
