use std::env;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::process::Command;

fn main() {
    let cargo_package = env::var("CARGO_PKG_NAME").expect("CARGO_PKG_NAME not defined");
    let cargo_version = env::var("CARGO_PKG_VERSION").expect("CARGO_PKG_VERSION not defined");
    let git_version = match Command::new("git").arg("rev-parse").arg("HEAD").output() {
        Ok(o) => {
            let mut s = String::from_utf8_lossy(&o.stdout).into_owned();
            s.pop();
            s
        }
        Err(_) => String::from("n/a"),
    };

    let out_dir = env::var("OUT_DIR").unwrap();
    let dest_path = Path::new(&out_dir).join("release.rs");
    let mut f = File::create(dest_path).unwrap();

    let ver_fn = format!(
        "pub fn version() -> &'static str {{ \"{pkg}@{ver}+{sha}\" }} \
        pub fn release_version() -> &'static str {{ \"{ver}+{sha}\" }}
        pub fn sentry_release_version() -> Option<std::borrow::Cow<'static, str>> {{ Some(std::borrow::Cow::Borrowed(release_version())) }}",
        pkg=cargo_package, ver=cargo_version, sha=git_version
    );

    f.write_all(ver_fn.as_bytes()).unwrap();
}
