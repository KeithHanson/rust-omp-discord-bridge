use std::os::unix::{
    fs::{OpenOptionsExt, PermissionsExt},
    process::CommandExt,
};
use std::{collections::HashMap, fs, io::Write, path::PathBuf, process::Command};

use anyhow::{Context, Result, bail};
use clap::{Parser, ValueEnum};
use omp_client_bridge::config::AppConfig;

#[derive(Parser)]
#[command(about = "Generate local bridge configuration from a protected .env and launch")]
struct Cli {
    #[arg(long, default_value = ".env")]
    env_file: PathBuf,
    #[arg(value_enum, default_value = "run")]
    command: Action,
}

#[derive(Clone, Copy, ValueEnum)]
enum Action {
    Check,
    Run,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let metadata = fs::metadata(&cli.env_file)
        .context("cannot read .env; copy .env.example and chmod 600 .env")?;
    if metadata.permissions().mode() & 0o077 != 0 {
        bail!(".env grants group or other access; run chmod 600 on it");
    }
    // Parse without exporting secrets to OMP or executing shell expressions.
    let values: HashMap<String, String> = dotenvy::from_path_iter(&cli.env_file)
        .map_err(|_| anyhow::anyhow!("cannot open .env"))?
        .collect::<std::result::Result<_, _>>()
        .map_err(|_| anyhow::anyhow!("invalid .env syntax (values withheld)"))?;
    let required = |key: &str| -> Result<&str> {
        values
            .get(key)
            .map(String::as_str)
            .filter(|v| !v.trim().is_empty())
            .with_context(|| format!("set {key} in {}", cli.env_file.display()))
    };
    let token = required("DISCORD_BOT_TOKEN")?;
    if token.len() > 4096 || token.bytes().any(|b| b.is_ascii_whitespace()) {
        bail!("DISCORD_BOT_TOKEN must be at most 4096 bytes and contain no whitespace");
    }
    for key in ["DISCORD_GUILD_ID", "DISCORD_CHANNEL_ID", "DISCORD_USER_ID"] {
        if required(key)?
            .parse::<u64>()
            .ok()
            .filter(|id| *id != 0)
            .is_none()
        {
            bail!("{key} must be a nonzero numeric Discord ID");
        }
    }
    let home = PathBuf::from(std::env::var_os("HOME").context("HOME is unset")?);
    let directory = home.join(".config/omp-client-bridge/local-env");
    fs::create_dir_all(&directory)?;
    fs::set_permissions(&directory, fs::Permissions::from_mode(0o700))?;
    let token_path = directory.join("discord-token");
    let config_path = directory.join("config.toml");
    let mut config: toml::Value = toml::from_str(include_str!("../../config.example.toml"))?;
    config["discord"]["token_file"] =
        toml::Value::String(token_path.to_string_lossy().into_owned());
    let guild = &mut config["discord"]["guilds"][0];
    guild["id"] = required("DISCORD_GUILD_ID")?.into();
    guild["allowed_channels"] = toml::Value::Array(vec![required("DISCORD_CHANNEL_ID")?.into()]);
    guild["allowed_users"] = toml::Value::Array(vec![required("DISCORD_USER_ID")?.into()]);
    guild["cwd"] = required("OMP_WORKDIR")?.into();
    config["omp"]["allowed_root"] = required("OMP_ALLOWED_ROOT")?.into();
    for (key, field) in [("OMP_EXECUTABLE", "executable"), ("OMP_MODEL", "model")] {
        if let Some(value) = values.get(key).filter(|v| !v.is_empty()) {
            config["omp"][field] = value.as_str().into();
        }
    }
    fs::write(&config_path, toml::to_string_pretty(&config)?)?;
    let validated = AppConfig::load(&config_path)?;
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&token_path)?;
    file.set_permissions(fs::Permissions::from_mode(0o600))?;
    file.write_all(token.as_bytes())?;
    drop(file);
    let discord = validated.discord.as_ref().context("the local launcher requires a Discord configuration")?;
    drop(discord.load_token()?);
    let binary = std::env::current_exe()?.with_file_name("omp-client-bridge");
    let error = Command::new(binary)
        .arg("--config")
        .arg(config_path)
        .arg(match cli.command {
            Action::Check => "check",
            Action::Run => "run",
        })
        .exec();
    Err(error)
        .context("cannot launch bridge; build both binaries with cargo build --release --bins")
}
