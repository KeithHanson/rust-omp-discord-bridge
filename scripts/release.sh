#!/usr/bin/env bash
set -Eeuo pipefail
export LC_ALL=C

usage() {
    cat <<'EOF'
Usage: scripts/release.sh [--dry-run] [--help]

Build a committed, tagged release for Linux x86-64 using static musl linking.
The clean checkout's VERSION must match a local vVERSION tag at HEAD.

  --dry-run  Build and package locally; do not contact GitHub or publish.
  --help     Show this help without building or publishing.

Publishing requires the same tag to be pushed to
KeithHanson/rust-omp-discord-bridge and an authenticated GitHub CLI.
Existing releases (including drafts) are never overwritten or resumed.
Artifacts are retained in target/releases/vVERSION.XXXXXX/.
EOF
}

fail() {
    printf 'release: %s\n' "$*" >&2
    exit 1
}

require() {
    command -v "$1" >/dev/null 2>&1 || fail "Missing $1. $2"
}

dry_run=false
for argument in "$@"; do
    case "$argument" in
        --dry-run) dry_run=true ;;
        --help|-h) usage; exit 0 ;;
        *) usage >&2; fail "Unknown argument: $argument" ;;
    esac
done

require git 'Install Git.'
require cargo 'Install Rust using rustup (https://rustup.rs/).'
require rustc 'Install Rust using rustup (https://rustup.rs/).'
require rustup 'Install Rust using rustup (https://rustup.rs/).'
require musl-gcc 'Install musl (Arch: sudo pacman -S musl; Debian/Ubuntu: sudo apt install musl-tools).'
require readelf 'Install binutils (Arch: sudo pacman -S binutils; Debian/Ubuntu: sudo apt install binutils).'
for utility in tar gzip sha256sum date mktemp install mkdir rm uname dirname; do
    require "$utility" 'Install GNU coreutils, GNU tar, and gzip.'
done
[[ $(uname -s) == Linux && $(uname -m) == x86_64 ]] || fail 'Run this script on Linux x86-64.'

root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)
cd -- "$root"
[[ $(git rev-parse --show-toplevel) == "$root" ]] || fail 'Run from a Git checkout of this repository.'
checkout_status=$(git status --porcelain --untracked-files=all)
[[ -z $checkout_status ]] || fail 'Commit or remove all tracked changes and untracked files before releasing.'
commit=$(git rev-parse --verify HEAD)
version=$(git show "$commit:VERSION")
[[ $version =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}-[1-9][0-9]*$ ]] || fail 'Committed VERSION must be YYYY-MM-DD-N with a positive counter.'
version_date=${version%-*}
[[ ${version:0:4} != 0000 ]] || fail 'VERSION must contain a valid calendar date.'
normalized_date=$(date --date="$version_date" +%F) || fail 'VERSION must contain a valid calendar date.'
[[ $normalized_date == "$version_date" ]] || fail 'VERSION must contain a valid calendar date.'
tag="v$version"
tag_commit=$(git rev-parse --verify "refs/tags/$tag^{commit}") || fail "Create the local tag first: git tag -a $tag -m '$tag'"
[[ $tag_commit == "$commit" ]] || fail "$tag does not point at HEAD; check out the intended release commit."

target=x86_64-unknown-linux-musl
installed_targets=$(rustup target list --installed)
[[ $'\n'$installed_targets$'\n' == *$'\n'"$target"$'\n'* ]] || fail "Install the target: rustup target add $target"
host=$(rustc -vV)
host=${host#*$'\nhost: '}
host=${host%%$'\n'*}
linker="$(rustc --print sysroot)/lib/rustlib/$host/bin/rust-lld"
[[ -x $linker ]] || fail 'The selected Rust toolchain lacks its bundled rust-lld linker; reinstall the toolchain.'
repository=KeithHanson/rust-omp-discord-bridge
export GH_HOST=github.com
if ! "$dry_run"; then
    require gh 'Install GitHub CLI, then run gh auth login --hostname github.com.'
    gh auth status --hostname github.com || fail 'Authenticate with gh auth login --hostname github.com (release write permission is required).'
    gh api "repos/$repository/git/ref/tags/$tag" --jq .ref >/dev/null \
        || fail "Push the release tag to $repository first: git push origin refs/tags/$tag"
    remote_commit=$(gh api "repos/$repository/commits/$tag" --jq .sha)
    [[ $remote_commit == "$commit" ]] || fail "GitHub's $tag points at another commit; refusing to publish."
    existing=$(gh api --paginate "repos/$repository/releases?per_page=100" \
        --jq ".[] | select(.tag_name == \"$tag\") | .id")
    [[ -z $existing ]] || fail "Release $tag already exists (possibly a draft); inspect it manually. Never overwrite a published release."
fi

work=$(mktemp -d "${TMPDIR:-/tmp}/omp-bridge-release.XXXXXX")
draft_created=false
published=false
cleanup() {
    status=$?
    trap - EXIT
    rm -rf -- "$work"
    if "$draft_created" && ! "$published"; then
        printf 'Release %s was created as a draft and was not confirmed published. Inspect https://github.com/%s/releases before retrying; this script will not overwrite it.\n' "$tag" "$repository" >&2
    fi
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
mkdir -- "$work/source"
# Build only the tagged tree: ignored files and concurrent checkout edits cannot
# accidentally become part of a release carrying this commit's identity.
git archive "$commit" | tar -xf - -C "$work/source"
(
    cd -- "$work/source"
    export BRIDGE_BUILD_REVISION="$commit"
    export CC_x86_64_unknown_linux_musl=musl-gcc
    # musl-gcc's specs can inject an ELF interpreter into Rust static PIEs.
    # Use Rust's bundled linker for the final link; musl-gcc only compiles C.
    export CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER="$linker"
    export CARGO_ENCODED_RUSTFLAGS=$'-C\x1ftarget-feature=+crt-static'
    cargo build --locked --release --bins --target "$target" --target-dir "$work/build"
)

binaries="$work/build/$target/release"
for binary in omp-discord-bridge bridge-local; do
    headers=$(readelf --program-headers "$binaries/$binary")
    dynamic=$(readelf --dynamic "$binaries/$binary")
    [[ $headers != *INTERP* && $dynamic != *'(NEEDED)'* ]] || fail "$binary is not a self-contained static executable."
done
actual_version=$("$binaries/omp-discord-bridge" --version)
expected_version="omp-discord-bridge $version (${commit:0:7})"
[[ $actual_version == "$expected_version" ]] || fail "Built binary reports '$actual_version', expected '$expected_version'."

bundle="omp-discord-bridge-$version-linux-x86_64"
raw_asset=omp-discord-bridge-linux-x86_64
mkdir -p -- "$root/target/releases"
artifacts=$(mktemp -d "$root/target/releases/$tag.XXXXXX")
mkdir -- "$work/$bundle"
install -m 755 "$binaries/omp-discord-bridge" "$binaries/bridge-local" "$work/$bundle/"
for file in .env.example config.example.toml config.container.example.toml omp-discord-bridge.service README.md LICENSE; do
    install -m 644 "$work/source/$file" "$work/$bundle/"
done
mkdir -p -- "$work/$bundle/licenses/coba"
for file in LICENSE NOTICE; do
    install -m 644 "$work/source/assets/coba/$file" "$work/$bundle/licenses/coba/"
done
install -m 755 "$binaries/omp-discord-bridge" "$artifacts/$raw_asset"
# Stable archive metadata; the executables still depend on the Rust toolchain.
epoch=$(git show -s --format=%ct "$commit")
tar --sort=name --mtime="@$epoch" --owner=0 --group=0 --numeric-owner \
    -C "$work" -cf - "$bundle" | gzip -n > "$artifacts/$bundle.tar.gz"
(
    cd -- "$artifacts"
    sha256sum "$raw_asset" "$bundle.tar.gz" > SHA256SUMS
    sha256sum --check SHA256SUMS
)
printf 'Artifacts: %s\n' "$artifacts"
if "$dry_run"; then
    printf 'Dry run complete; no GitHub release was created or changed.\n'
    exit 0
fi

# No --clobber, no draft resumption, and no automatic deletion on failure.
# Readers cannot discover this through the latest-release API until every
# upload has succeeded and the final publish call completes.
gh release create "$tag" --repo "$repository" --verify-tag --draft \
    --title "$tag" --notes "Release $version from commit $commit. Static Linux x86-64 binaries; OMP must be installed separately. See README.md for installation and upgrade instructions."
draft_created=true
gh release upload "$tag" --repo "$repository" \
    "$artifacts/$raw_asset" "$artifacts/$bundle.tar.gz" "$artifacts/SHA256SUMS"
gh release edit "$tag" --repo "$repository" --draft=false --latest
published=true
printf 'Published https://github.com/%s/releases/tag/%s\n' "$repository" "$tag"
