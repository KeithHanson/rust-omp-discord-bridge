use std::{env, process::ExitCode};

const USAGE: &str = "Usage: omp-discord-bridge [--help | --version]";

fn main() -> ExitCode {
    let mut args = env::args();
    let _program = args.next();
    let argument = args.next();

    if args.next().is_some() {
        eprintln!("error: expected at most one argument\n\n{USAGE}");
        return ExitCode::from(2);
    }

    match argument.as_deref() {
        None | Some("-h" | "--help") => {
            println!("{USAGE}");
            ExitCode::SUCCESS
        }
        Some("-V" | "--version") => {
            println!("omp-discord-bridge {}", env!("CARGO_PKG_VERSION"));
            ExitCode::SUCCESS
        }
        Some(argument) => {
            eprintln!("error: unknown argument `{argument}`\n\n{USAGE}");
            ExitCode::from(2)
        }
    }
}
