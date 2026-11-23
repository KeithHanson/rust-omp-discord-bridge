use std::{env, path::Path, process::Command};

fn git(root: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .current_dir(root)
        .args(args)
        .output()
        .expect("Git is required unless BRIDGE_BUILD_REVISION is supplied");
    assert!(
        output.status.success(),
        "Cannot determine build revision; source archives must supply BRIDGE_BUILD_REVISION"
    );
    String::from_utf8(output.stdout)
        .expect("Git returned non-UTF-8 build metadata")
        .trim()
        .to_owned()
}

fn release_version(root: &Path) -> String {
    let version = std::fs::read_to_string(root.join("VERSION")).expect("VERSION is required");
    let version = version.trim();
    let mut fields = version.split('-');
    let year = fields.next().unwrap_or("");
    let month = fields.next().unwrap_or("");
    let day = fields.next().unwrap_or("");
    let release = fields.next().unwrap_or("");
    assert!(
        year.len() == 4
            && month.len() == 2
            && day.len() == 2
            && !release.is_empty()
            && !release.starts_with('0')
            && fields.next().is_none()
            && [year, month, day, release]
                .iter()
                .all(|field| field.bytes().all(|byte| byte.is_ascii_digit())),
        "VERSION must use YYYY-MM-DD-# with a positive release counter"
    );
    let year: u32 = year.parse().unwrap();
    let month: u32 = month.parse().unwrap();
    let day: u32 = day.parse().unwrap();
    let days = match month {
        2 if year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400)) => {
            29
        }
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        _ => 0,
    };
    assert!(
        year > 0 && day > 0 && day <= days,
        "VERSION must contain a valid calendar date"
    );
    version.to_owned()
}

fn main() {
    println!("cargo:rerun-if-env-changed=BRIDGE_BUILD_REVISION");
    for path in ["build.rs", "Cargo.toml", "Cargo.lock", "VERSION", "src"] {
        println!("cargo:rerun-if-changed={path}");
    }
    let root = env::var_os("CARGO_MANIFEST_DIR").expect("Cargo manifest directory");
    let root = Path::new(&root);
    let version = release_version(root);
    println!("cargo:rustc-env=BRIDGE_RELEASE_VERSION={version}");
    let revision = match env::var("BRIDGE_BUILD_REVISION") {
        Ok(value) if !value.is_empty() => value,
        _ => {
            let git_root = git(root, &["rev-parse", "--show-toplevel"]);
            assert_eq!(
                Path::new(&git_root).canonicalize().unwrap(),
                root.canonicalize().unwrap(),
                "Build is not at its Git repository root; supply BRIDGE_BUILD_REVISION"
            );
            // Cover normal clones, detached HEADs, packed refs, and Git worktrees.
            for name in ["HEAD", "index", "refs", "packed-refs"] {
                let path = root.join(git(root, &["rev-parse", "--git-path", name]));
                if path.exists() {
                    println!("cargo:rerun-if-changed={}", path.display());
                }
            }
            let mut revision = git(root, &["rev-parse", "HEAD"]);
            let dirty = git(
                root,
                &[
                    "status",
                    "--porcelain",
                    "--untracked-files=normal",
                    "--",
                    "build.rs",
                    "Cargo.toml",
                    "Cargo.lock",
                    "VERSION",
                    "src",
                ],
            );
            if !dirty.is_empty() {
                revision.push_str("-dirty");
            }
            revision
        }
    };
    let (hash, suffix) = revision
        .strip_suffix("-dirty")
        .map_or((revision.as_str(), ""), |hash| (hash, "-dirty"));
    assert!(
        (7..=64).contains(&hash.len()) && hash.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "BRIDGE_BUILD_REVISION must be a Git hash (at least 7 hex digits), optionally ending in -dirty"
    );
    let short = format!("{}{suffix}", hash[..7].to_ascii_lowercase());
    println!("cargo:rustc-env=BRIDGE_BUILD_REVISION={short}");
    println!("cargo:rustc-env=BRIDGE_BUILD_VERSION={version} ({short})");
}
