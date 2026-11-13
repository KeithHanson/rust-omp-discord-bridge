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

fn main() {
    println!("cargo:rerun-if-env-changed=BRIDGE_BUILD_REVISION");
    for path in ["build.rs", "Cargo.toml", "Cargo.lock", "src"] {
        println!("cargo:rerun-if-changed={path}");
    }
    let root = env::var_os("CARGO_MANIFEST_DIR").expect("Cargo manifest directory");
    let root = Path::new(&root);
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
            let dirty = git(root, &[
                "status", "--porcelain", "--untracked-files=normal", "--",
                "build.rs", "Cargo.toml", "Cargo.lock", "src",
            ]);
            if !dirty.is_empty() {
                revision.push_str("-dirty");
            }
            revision
        }
    };
    let (hash, suffix) = revision.strip_suffix("-dirty")
        .map_or((revision.as_str(), ""), |hash| (hash, "-dirty"));
    assert!(
        (7..=64).contains(&hash.len()) && hash.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "BRIDGE_BUILD_REVISION must be a Git hash (at least 7 hex digits), optionally ending in -dirty"
    );
    let short = format!("{}{suffix}", hash[..7].to_ascii_lowercase());
    println!("cargo:rustc-env=BRIDGE_BUILD_REVISION={short}");
    println!(
        "cargo:rustc-env=BRIDGE_BUILD_VERSION={} ({short})",
        env::var("CARGO_PKG_VERSION").expect("Cargo package version")
    );
}
