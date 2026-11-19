//! Download and preflight release assets while running; replace only after drain.

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
mod supported {
    use std::{
        env,
        fs::{self, File, OpenOptions},
        io::{self, Write},
        os::unix::{fs::{MetadataExt, OpenOptionsExt, PermissionsExt}, process::CommandExt},
        path::{Path, PathBuf},
        process::Stdio,
        time::Duration,
    };

    use anyhow::{Context, Result, bail, ensure};
    use reqwest::{Client, header};
    use serde::Deserialize;
    use sha2::{Digest, Sha256};
    use tempfile::TempDir;
    use tokio::{io::AsyncReadExt, process::Command, time};

    const REPOSITORY: &str = "KeithHanson/rust-omp-discord-bridge";
    const ASSET: &str = "omp-client-bridge-linux-x86_64";
    const POLL_INTERVAL: Duration = Duration::from_secs(420);
    const CHECK_TIMEOUT: Duration = Duration::from_secs(60);
    const METADATA_LIMIT: usize = 1024 * 1024;
    const CHECKSUM_LIMIT: usize = 64 * 1024;
    const BINARY_LIMIT: usize = 128 * 1024 * 1024;

    #[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
    struct ReleaseVersion(u32, u32, u32, u64);

    impl ReleaseVersion {
        fn parse(text: &str) -> Result<Self> {
            let mut fields = text.split('-');
            let (Some(year), Some(month), Some(day), Some(counter)) =
                (fields.next(), fields.next(), fields.next(), fields.next()) else {
                    bail!("invalid release version");
                };
            ensure!(fields.next().is_none() && year.len() == 4 && month.len() == 2 && day.len() == 2
                && !counter.is_empty() && !counter.starts_with('0')
                && [year, month, day, counter].iter().all(|field| field.bytes().all(|byte| byte.is_ascii_digit())),
                "invalid release version");
            let year: u32 = year.parse()?;
            let month: u32 = month.parse()?;
            let day: u32 = day.parse()?;
            let counter: u64 = counter.parse()?;
            let days = match month {
                2 if year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400)) => 29,
                2 => 28,
                4 | 6 | 9 | 11 => 30,
                1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
                _ => 0,
            };
            ensure!(year > 0 && day > 0 && day <= days, "invalid release calendar date");
            Ok(Self(year, month, day, counter))
        }
    }

    #[derive(Deserialize)]
    struct Release {
        tag_name: String,
        draft: bool,
        prerelease: bool,
        assets: Vec<Asset>,
    }

    #[derive(Deserialize)]
    struct Asset {
        id: u64,
        name: String,
        size: u64,
    }

    impl Release {
        fn asset(&self, name: &str, limit: usize) -> Result<&Asset> {
            let mut matches = self.assets.iter().filter(|asset| asset.name == name);
            let asset = matches.next().context("release asset missing")?;
            ensure!(matches.next().is_none(), "duplicate release asset");
            ensure!(asset.size > 0 && asset.size <= limit as u64, "invalid release asset size");
            Ok(asset)
        }
    }

    // Only tests can supply another origin/client: production always uses HTTPS GitHub API.
    struct Github {
        client: Client,
        origin: String,
    }

    impl Github {
        fn production() -> Result<Self> {
            let mut headers = header::HeaderMap::new();
            headers.insert("X-GitHub-Api-Version", header::HeaderValue::from_static("2022-11-28"));
            if let Some(token) = env::var_os("OMP_CLIENT_BRIDGE_UPDATE_TOKEN") {
                let token = token.into_string().map_err(|_| anyhow::anyhow!("invalid update token encoding"))?;
                let mut value = header::HeaderValue::from_str(&format!("Bearer {token}"))
                    .map_err(|_| anyhow::anyhow!("invalid update token header"))?;
                value.set_sensitive(true);
                headers.insert(header::AUTHORIZATION, value);
            }
            let client = Client::builder()
                .user_agent("omp-client-bridge-updater")
                .default_headers(headers)
                .https_only(true)
                .connect_timeout(Duration::from_secs(10))
                .read_timeout(Duration::from_secs(30))
                .timeout(Duration::from_secs(180))
                .redirect(reqwest::redirect::Policy::limited(5))
                .build()?;
            Ok(Self { client, origin: "https://api.github.com".to_owned() })
        }

        async fn response(&self, path: &str, accept: &'static str, limit: usize) -> Result<reqwest::Response> {
            let response = self.client.get(format!("{}{path}", self.origin))
                .header(header::ACCEPT, accept)
                .send().await?.error_for_status()?;
            ensure!(response.content_length().is_none_or(|size| size <= limit as u64),
                "release response exceeds size limit");
            Ok(response)
        }

        async fn bytes(&self, path: &str, accept: &'static str, limit: usize) -> Result<Vec<u8>> {
            let mut response = self.response(path, accept, limit).await?;
            let mut bytes = Vec::new();
            while let Some(chunk) = response.chunk().await? {
                ensure!(chunk.len() <= limit - bytes.len(), "release response exceeds size limit");
                bytes.extend_from_slice(&chunk);
            }
            Ok(bytes)
        }

        async fn prepare(&self, current: ReleaseVersion, executable: &Path, config: &Path) -> Result<Option<PreparedUpdate>> {
            let bytes = self.bytes(&format!("/repos/{REPOSITORY}/releases/latest"),
                "application/vnd.github+json", METADATA_LIMIT).await?;
            let release: Release = serde_json::from_slice(&bytes).context("invalid GitHub release metadata")?;
            if release.draft || release.prerelease {
                return Ok(None);
            }
            let version = release.tag_name.strip_prefix('v').context("release tag must start with v")?;
            if ReleaseVersion::parse(version)? <= current {
                return Ok(None);
            }
            let binary = release.asset(ASSET, BINARY_LIMIT)?;
            let checksums = release.asset("SHA256SUMS", CHECKSUM_LIMIT)?;
            let sums = self.bytes(&format!("/repos/{REPOSITORY}/releases/assets/{}", checksums.id),
                "application/octet-stream", CHECKSUM_LIMIT).await?;
            ensure!(sums.len() as u64 == checksums.size, "checksum asset size mismatch");
            let expected = checksum(&sums)?;
            let metadata = fs::metadata(executable).context("reading installed binary")?;
            ensure!(metadata.is_file(), "installed executable is not a regular file");
            let parent = executable.parent().context("executable has no directory")?;
            let staging = tempfile::Builder::new().prefix(".omp-update-").tempdir_in(parent)
                .context("creating update staging directory beside executable")?;
            fs::set_permissions(staging.path(), fs::Permissions::from_mode(0o700))?;
            let candidate = staging.path().join(ASSET);
            let mut file = OpenOptions::new().write(true).create_new(true).mode(0o600).open(&candidate)?;
            let mut response = self.response(&format!("/repos/{REPOSITORY}/releases/assets/{}", binary.id),
                "application/octet-stream", BINARY_LIMIT).await?;
            let mut size = 0usize;
            let mut digest = Sha256::new();
            while let Some(chunk) = response.chunk().await? {
                ensure!(chunk.len() <= BINARY_LIMIT - size, "binary exceeds size limit");
                size += chunk.len();
                digest.update(&chunk);
                file.write_all(&chunk)?;
            }
            ensure!(size as u64 == binary.size, "binary asset size mismatch");
            ensure!(digest.finalize().as_slice() == expected, "release binary SHA256 mismatch");
            file.set_permissions(fs::Permissions::from_mode(0o700))?;
            file.sync_all()?;
            drop(file);
            verify_version(&candidate, version).await?;
            Ok(Some(PreparedUpdate {
                staging,
                candidate,
                executable: executable.to_owned(),
                config: config.to_owned(),
                version: version.to_owned(),
                identity: (metadata.dev(), metadata.ino()),
                checked: false,
            }))
        }
    }

    fn checksum(bytes: &[u8]) -> Result<[u8; 32]> {
        let text = std::str::from_utf8(bytes).context("SHA256SUMS is not UTF-8")?;
        let mut found = None;
        for line in text.lines().filter(|line| !line.is_empty()) {
            let bytes = line.as_bytes();
            ensure!(bytes.len() >= 67 && bytes[64] == b' ' && matches!(bytes[65], b' ' | b'*'),
                "invalid SHA256SUMS entry");
            ensure!(bytes[..64].iter().all(u8::is_ascii_hexdigit), "invalid SHA256 digest");
            if &bytes[66..] != ASSET.as_bytes() {
                continue;
            }
            ensure!(found.is_none(), "duplicate binary checksum entry");
            let mut digest = [0u8; 32];
            for (index, pair) in bytes[..64].chunks_exact(2).enumerate() {
                let digit = |byte: u8| if byte.is_ascii_digit() { byte - b'0' } else { byte.to_ascii_lowercase() - b'a' + 10 };
                digest[index] = digit(pair[0]) * 16 + digit(pair[1]);
            }
            found = Some(digest);
        }
        found.context("SHA256SUMS lacks exact binary asset entry")
    }

    fn version_matches(output: &[u8], expected: &str) -> bool {
        let Ok(text) = std::str::from_utf8(output) else { return false; };
        let Some(revision) = text.trim_end_matches(['\r', '\n'])
            .strip_prefix("omp-client-bridge ")
            .and_then(|text| text.strip_prefix(expected))
            .and_then(|text| text.strip_prefix(" ("))
            .and_then(|text| text.strip_suffix(')')) else { return false; };
        revision.len() == 7 && revision.bytes().all(|byte| byte.is_ascii_hexdigit())
    }

    async fn verify_version(candidate: &Path, expected: &str) -> Result<()> {
        let mut child = Command::new(candidate).arg("--version")
            .stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::null())
            .kill_on_drop(true).spawn().context("starting candidate version check")?;
        let mut stdout = child.stdout.take().context("candidate stdout missing")?.take(1025);
        let mut output = Vec::new();
        let result = time::timeout(Duration::from_secs(10), async {
            stdout.read_to_end(&mut output).await?;
            ensure!(output.len() <= 1024, "candidate version output too large");
            let status = child.wait().await?;
            ensure!(status.success() && version_matches(&output, expected), "candidate version does not match release");
            Ok(())
        }).await;
        match result {
            Ok(Ok(())) => Ok(()),
            result => {
                let _ = child.kill().await;
                match result {
                    Ok(Err(error)) => Err(error),
                    Err(_) => bail!("candidate version check timed out"),
                    Ok(Ok(())) => unreachable!(),
                }
            }
        }
    }

    /// A verified download. Dropping it removes staging files without touching the running binary.
    pub struct PreparedUpdate {
        staging: TempDir,
        candidate: PathBuf,
        executable: PathBuf,
        config: PathBuf,
        version: String,
        identity: (u64, u64),
        checked: bool,
    }

    impl PreparedUpdate {
        /// Check configuration without opening or migrating SQLite; safe before the drain gate.
        pub async fn preflight(&mut self) -> Result<()> {
            self.preflight_with_timeout(CHECK_TIMEOUT).await
        }

        async fn preflight_with_timeout(&mut self, timeout: Duration) -> Result<()> {
            self.checked = false;
            let mut child = Command::new(&self.candidate)
                .arg("--config").arg(&self.config).arg("check-update")
                .env("OMP_CLIENT_BRIDGE_AUTO_UPDATE", "false")
                .stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null())
                .kill_on_drop(true).spawn().context("starting candidate configuration check")?;
            match time::timeout(timeout, child.wait()).await {
                Ok(status) => ensure!(status?.success(), "candidate configuration check failed"),
                Err(_) => {
                    let _ = child.kill().await;
                    bail!("candidate configuration check timed out");
                }
            }
            self.checked = true;
            Ok(())
        }

        /// Atomically replace the binary and exec with the original argv and environment.
        /// A failed exec restores the old binary; the caller can resume admission on any error.
        pub fn install_and_exec(self) -> Result<()> {
            let mut args = env::args_os();
            self.install_with(|path| {
                let mut command = std::process::Command::new(path);
                if let Some(arg0) = args.next() {
                    command.arg0(arg0);
                }
                command.args(args).exec()
            })
        }

        fn install_with(self, exec: impl FnOnce(&Path) -> io::Error) -> Result<()> {
            ensure!(self.checked, "candidate configuration check has not passed");
            let metadata = fs::metadata(&self.executable)?;
            ensure!((metadata.dev(), metadata.ino()) == self.identity, "installed executable changed during update");
            let directory = File::open(self.executable.parent().context("executable has no directory")?)?;
            let mut backup_name = self.executable.file_name().context("executable has no filename")?.to_os_string();
            backup_name.push(".previous");
            let backup = self.executable.with_file_name(backup_name);
            let staged_backup = self.staging.path().join("previous");
            fs::hard_link(&self.executable, &staged_backup).context("preserving installed executable")?;
            File::open(&staged_backup)?.sync_all()?;
            fs::rename(&staged_backup, &backup).context("saving previous executable")?;
            directory.sync_all()?;
            fs::rename(&self.candidate, &self.executable).context("atomically installing update")?;
            let result = (|| -> Result<()> {
                directory.sync_all().context("syncing installed update")?;
                self.staging.close().context("removing update staging directory")?;
                tracing::info!(version = %self.version, "executing bridge update");
                Err(exec(&self.executable)).context("executing installed update")
            })();
            // exec only returns on failure. Keep the old process and its executable usable.
            let error = result.expect_err("exec cannot return successfully");
            fs::rename(&backup, &self.executable)
                .with_context(|| format!("{error:#}; rollback failed; old binary retained at {}", backup.display()))?;
            directory.sync_all().with_context(|| format!("{error:#}; restored binary but directory sync failed"))?;
            Err(error)
        }
    }

    /// Poll the latest stable release. Disabled configurations remain pending until cancelled.
    pub async fn wait_for_update(config_path: &Path) -> PreparedUpdate {
        let setting = env::var("OMP_CLIENT_BRIDGE_AUTO_UPDATE").ok();
        let disabled = setting.as_deref().is_some_and(|value| value.eq_ignore_ascii_case("false"))
            || env!("BRIDGE_BUILD_REVISION").ends_with("-dirty")
            || (cfg!(debug_assertions) && !setting.as_deref().is_some_and(|value| value.eq_ignore_ascii_case("true")));
        if disabled {
            tracing::info!("automatic updates disabled (setting, development, or dirty build)");
            return std::future::pending().await;
        }
        let setup = (|| -> Result<_> {
            let executable = env::current_exe().context("locating running executable")?.canonicalize()?;
            let config = if config_path.is_absolute() { config_path.to_owned() } else { env::current_dir()?.join(config_path) };
            Ok((Github::production()?, ReleaseVersion::parse(env!("BRIDGE_RELEASE_VERSION"))?, executable, config))
        })();
        let (github, current, executable, config) = match setup {
            Ok(setup) => setup,
            Err(error) => {
                tracing::warn!(error = %error, "automatic updates unavailable");
                return std::future::pending().await;
            }
        };
        loop {
            // Also backs off a candidate that failed the caller's preflight.
            time::sleep(POLL_INTERVAL).await;
            match github.prepare(current, &executable, &config).await {
                Ok(Some(update)) => return update,
                Ok(None) => {}
                Err(error) => tracing::warn!(error = %error, "automatic update check failed; keeping current bridge"),
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::{io::{Read, Write}, net::TcpListener, thread};

        fn executable(path: &Path, contents: &[u8]) {
            fs::write(path, contents).unwrap();
            fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
        }

        fn prepared(root: &Path, script: &[u8]) -> PreparedUpdate {
            let target = root.join("bridge");
            executable(&target, b"old binary");
            let metadata = fs::metadata(&target).unwrap();
            let staging = tempfile::Builder::new().prefix(".omp-update-").tempdir_in(root).unwrap();
            let candidate = staging.path().join(ASSET);
            executable(&candidate, script);
            PreparedUpdate {
                staging, candidate, executable: target, config: root.join("config with spaces.toml"),
                version: "2026-11-14-10".into(), identity: (metadata.dev(), metadata.ino()), checked: false,
            }
        }

        #[test]
        fn versions_use_calendar_and_numeric_counter() {
            let parse = ReleaseVersion::parse;
            assert!(parse("2026-11-14-10").unwrap() > parse("2026-11-14-9").unwrap());
            assert!(parse("2027-01-01-1").unwrap() > parse("2026-12-31-999").unwrap());
            assert!(parse("2024-02-29-1").is_ok());
            for invalid in ["2026-02-29-1", "1900-02-29-1", "0000-01-01-1", "2026-13-01-1", "2026-01-00-1", "2026-1-01-1", "2026-01-01-0", "2026-01-01-01", "2026-01-01-18446744073709551616", "v2026-01-01-1", "2026-01-01-1-rc"] {
                assert!(parse(invalid).is_err(), "{invalid}");
            }
        }

        #[test]
        fn checksum_requires_one_exact_asset() {
            let hash = "ab".repeat(32);
            for marker in [' ', '*'] {
                assert_eq!(checksum(format!("{hash} {marker}{ASSET}\n").as_bytes()).unwrap(), [0xab; 32]);
            }
            assert!(checksum(format!("{hash}  ./{ASSET}\n").as_bytes()).is_err());
            assert!(checksum(format!("{hash}  {ASSET}.sig\n").as_bytes()).is_err());
            assert!(checksum(format!("{hash}  {ASSET}\n{hash}  {ASSET}\n").as_bytes()).is_err());
            assert!(checksum(format!("{}  {ASSET}\n", "zz".repeat(32)).as_bytes()).is_err());
            assert!(checksum(b"broken").is_err());
        }

        #[test]
        fn version_output_is_exact_and_clean() {
            assert!(version_matches(b"omp-client-bridge 2026-11-14-10 (abcdef0)\n", "2026-11-14-10"));
            for output in ["omp-client-bridge 2026-11-14-100 (abcdef0)", "omp-client-bridge 2026-11-14-10 (abcdef0-dirty)", "other 2026-11-14-10 (abcdef0)", "omp-client-bridge 2026-11-14-10 (abcdef0)\nextra"] {
                assert!(!version_matches(output.as_bytes(), "2026-11-14-10"));
            }
        }

        #[tokio::test]
        async fn failed_preflight_keeps_old_binary_and_cleans_staging() {
            let root = tempfile::tempdir().unwrap();
            let mut update = prepared(root.path(), b"#!/bin/sh\nexit 1\n");
            let staging = update.staging.path().to_owned();
            assert!(update.preflight().await.is_err());
            assert!(!update.checked);
            assert_eq!(fs::read(&update.executable).unwrap(), b"old binary");
            assert!(update.install_with(|_| panic!("must not execute")).is_err());
            assert!(!staging.exists());
        }

        #[tokio::test]
        async fn preflight_uses_nonmutating_command_and_preserves_argument_boundaries() {
            let root = tempfile::tempdir().unwrap();
            let mut update = prepared(root.path(), b"#!/bin/sh\n[ \"$#\" = 3 ] && [ \"$1\" = --config ] && [ \"$3\" = check-update ] && [ \"$OMP_CLIENT_BRIDGE_AUTO_UPDATE\" = false ]\n");
            update.preflight().await.unwrap();
            assert!(update.checked);
        }

        #[tokio::test]
        async fn timed_out_preflight_keeps_old_binary() {
            let root = tempfile::tempdir().unwrap();
            let mut update = prepared(root.path(), b"#!/bin/sh\nexec sleep 60\n");
            assert!(update.preflight_with_timeout(Duration::from_millis(20)).await.is_err());
            assert!(!update.checked);
            assert_eq!(fs::read(&update.executable).unwrap(), b"old binary");
        }

        #[test]
        fn failed_exec_restores_old_binary_and_removes_staging() {
            let root = tempfile::tempdir().unwrap();
            let mut update = prepared(root.path(), b"new binary");
            update.checked = true;
            let staging = update.staging.path().to_owned();
            let target = update.executable.clone();
            assert!(update.install_with(|path| {
                assert_eq!(fs::read(path).unwrap(), b"new binary");
                assert_eq!(fs::read(path.with_file_name("bridge.previous")).unwrap(), b"old binary");
                io::Error::from_raw_os_error(libc::ENOEXEC)
            }).is_err());
            assert_eq!(fs::read(target).unwrap(), b"old binary");
            assert!(!staging.exists());
        }

        #[test]
        fn successful_exec_preserves_pid_and_arguments() {
            const CHILD_ROOT: &str = "BRIDGE_TEST_EXEC_ROOT";
            if let Some(root) = env::var_os(CHILD_ROOT) {
                let mut update = prepared(Path::new(&root),
                    b"#!/bin/sh\nprintf '%s\\n' \"$$\" \"$@\" > \"$BRIDGE_TEST_EXEC_ROOT/observed\"\n");
                update.checked = true;
                panic!("exec returned: {:?}", update.install_and_exec());
            }
            let root = tempfile::tempdir().unwrap();
            let arguments = [
                "--exact",
                "update::supported::tests::successful_exec_preserves_pid_and_arguments",
                "--nocapture",
            ];
            let mut child = std::process::Command::new(env::current_exe().unwrap())
                .args(arguments)
                .env(CHILD_ROOT, root.path())
                .stdout(Stdio::null())
                .spawn().unwrap();
            let pid = child.id();
            assert!(child.wait().unwrap().success());
            let observed = fs::read_to_string(root.path().join("observed")).unwrap();
            let lines: Vec<_> = observed.lines().collect();
            assert_eq!(lines[0], pid.to_string());
            assert_eq!(&lines[1..], &arguments);
            assert_eq!(fs::read(root.path().join("bridge.previous")).unwrap(), b"old binary");
            assert!(!fs::read(root.path().join("bridge")).unwrap().starts_with(b"old binary"));
        }

        #[test]
        fn failed_install_and_changed_executable_never_exec() {
            for replace_current in [false, true] {
                let root = tempfile::tempdir().unwrap();
                let mut update = prepared(root.path(), b"new binary");
                update.checked = true;
                let target = update.executable.clone();
                if replace_current {
                    let replacement = root.path().join("replacement");
                    executable(&replacement, b"external replacement");
                    fs::rename(replacement, &target).unwrap();
                } else {
                    fs::remove_file(&update.candidate).unwrap();
                }
                assert!(update.install_with(|_| panic!("must not execute")).is_err());
                assert_eq!(fs::read(target).unwrap(), if replace_current { &b"external replacement"[..] } else { &b"old binary"[..] });
            }
        }

        // An isolated HTTP fixture exercises the real HTTP/download path, without an insecure runtime override.
        fn server(responses: Vec<(u16, Vec<u8>)>) -> (Github, thread::JoinHandle<()>) {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let origin = format!("http://{}", listener.local_addr().unwrap());
            let handle = thread::spawn(move || {
                for (status, body) in responses {
                    let (mut stream, _) = listener.accept().unwrap();
                    stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
                    let mut request = Vec::new();
                    while !request.ends_with(b"\r\n\r\n") {
                        let mut byte = [0];
                        stream.read_exact(&mut byte).unwrap();
                        request.push(byte[0]);
                    }
                    write!(stream, "HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len()).unwrap();
                    stream.write_all(&body).unwrap();
                }
            });
            (Github { client: Client::builder().no_proxy().timeout(Duration::from_secs(5)).build().unwrap(), origin }, handle)
        }

        #[tokio::test]
        async fn local_http_download_verifies_and_stages_without_installing() {
            let root = tempfile::tempdir().unwrap();
            let target = root.path().join("bridge");
            executable(&target, b"old binary");
            let binary = b"#!/bin/sh\nprintf 'omp-client-bridge 2026-11-14-10 (abcdef0)\\n'\n";
            let sums = format!("{:x}  {ASSET}\n", Sha256::digest(binary)).into_bytes();
            let metadata = serde_json::to_vec(&serde_json::json!({
                "tag_name": "v2026-11-14-10", "draft": false, "prerelease": false,
                "assets": [{"id": 1, "name": ASSET, "size": binary.len()}, {"id": 2, "name": "SHA256SUMS", "size": sums.len()}]
            })).unwrap();
            let (github, handle) = server(vec![(200, metadata), (200, sums), (200, binary.to_vec())]);
            let update = github.prepare(ReleaseVersion::parse("2026-11-14-9").unwrap(), &target, &root.path().join("config")).await.unwrap().unwrap();
            handle.join().unwrap();
            assert!(!update.checked);
            assert_eq!(fs::read(&target).unwrap(), b"old binary");
            assert_eq!(fs::metadata(update.staging.path()).unwrap().permissions().mode() & 0o777, 0o700);
            let staging = update.staging.path().to_owned();
            drop(update);
            assert!(!staging.exists());
        }

        #[tokio::test]
        async fn corrupt_or_mislabeled_download_cleans_up_without_installing() {
            for wrong_checksum in [false, true] {
                let root = tempfile::tempdir().unwrap();
                let target = root.path().join("bridge");
                executable(&target, b"old binary");
                let binary = b"#!/bin/sh\nprintf 'omp-client-bridge 2026-11-14-9 (abcdef0)\\n'\n";
                let hash = if wrong_checksum { Sha256::digest(b"different binary") } else { Sha256::digest(binary) };
                let sums = format!("{hash:x}  {ASSET}\n").into_bytes();
                let metadata = serde_json::to_vec(&serde_json::json!({
                    "tag_name": "v2026-11-14-10", "draft": false, "prerelease": false,
                    "assets": [{"id": 1, "name": ASSET, "size": binary.len()}, {"id": 2, "name": "SHA256SUMS", "size": sums.len()}]
                })).unwrap();
                let (github, handle) = server(vec![(200, metadata), (200, sums), (200, binary.to_vec())]);
                assert!(github.prepare(ReleaseVersion::parse("2026-11-14-9").unwrap(), &target, root.path()).await.is_err());
                handle.join().unwrap();
                assert_eq!(fs::read(target).unwrap(), b"old binary");
                assert_eq!(fs::read_dir(root.path()).unwrap().count(), 1);
            }
        }

        #[tokio::test]
        async fn only_newer_stable_releases_are_considered() {
            for (tag, draft, prerelease) in [
                ("v2026-11-14-8", false, false),
                ("v2026-11-14-9", false, false),
                ("v2026-11-14-10", true, false),
                ("v2026-11-14-10", false, true),
            ] {
                let root = tempfile::tempdir().unwrap();
                let metadata = serde_json::to_vec(&serde_json::json!({
                    "tag_name": tag, "draft": draft, "prerelease": prerelease, "assets": [],
                })).unwrap();
                let (github, handle) = server(vec![(200, metadata)]);
                assert!(github.prepare(ReleaseVersion::parse("2026-11-14-9").unwrap(), &root.path().join("unused"), root.path()).await.unwrap().is_none());
                handle.join().unwrap();
                assert_eq!(fs::read_dir(root.path()).unwrap().count(), 0);
            }
        }

        #[test]
        fn release_assets_must_be_unique_and_bounded() {
            let release = Release {
                tag_name: "v2026-11-14-10".into(), draft: false, prerelease: false,
                assets: vec![Asset { id: 1, name: ASSET.into(), size: BINARY_LIMIT as u64 + 1 }],
            };
            assert!(release.asset(ASSET, BINARY_LIMIT).is_err());
            let release = Release {
                assets: vec![
                    Asset { id: 1, name: ASSET.into(), size: 1 },
                    Asset { id: 2, name: ASSET.into(), size: 1 },
                ],
                ..release
            };
            assert!(release.asset(ASSET, BINARY_LIMIT).is_err());
            assert!(release.asset("SHA256SUMS", CHECKSUM_LIMIT).is_err());
        }

        #[tokio::test]
        async fn http_failure_leaves_running_binary_unchanged() {
            let root = tempfile::tempdir().unwrap();
            let target = root.path().join("bridge");
            executable(&target, b"old binary");
            let (github, handle) = server(vec![(503, b"unavailable".to_vec())]);
            assert!(github.prepare(ReleaseVersion::parse("2026-11-14-9").unwrap(), &target, root.path()).await.is_err());
            handle.join().unwrap();
            assert_eq!(fs::read(target).unwrap(), b"old binary");
            assert_eq!(fs::read_dir(root.path()).unwrap().count(), 1);
        }
    }
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
pub use supported::wait_for_update;

#[cfg(not(all(target_os = "linux", target_arch = "x86_64")))]
mod unsupported {
    use std::path::Path;

    pub struct PreparedUpdate;

    impl PreparedUpdate {
        pub async fn preflight(&mut self) -> anyhow::Result<()> {
            anyhow::bail!("automatic updates are supported only on Linux x86_64")
        }

        pub fn install_and_exec(self) -> anyhow::Result<()> {
            anyhow::bail!("automatic updates are supported only on Linux x86_64")
        }
    }

    pub async fn wait_for_update(_: &Path) -> PreparedUpdate {
        tracing::info!("automatic updates disabled on unsupported platform");
        std::future::pending().await
    }
}

#[cfg(not(all(target_os = "linux", target_arch = "x86_64")))]
pub use unsupported::wait_for_update;
