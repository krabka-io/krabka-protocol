//! Remove only the two known rustdoc requests for unwritten empty-implementor assets.
//! Validate both pages before writing either; retain real assets and unrelated scripts.

use std::{env, fs, io, path::Path, process::ExitCode};

const TARGETS: [(&str, &str); 2] = [
    (
        "krabka-security/krabka_security/trait.IntrospectionClient.html",
        "../trait.impl/krabka_security/oauthbearer/trait.IntrospectionClient.js",
    ),
    (
        "krabka-metadata/krabka_metadata/trait.Feature.html",
        "../trait.impl/krabka_metadata/feature/trait.Feature.js",
    ),
];

fn repair(site: &Path) -> io::Result<usize> {
    let mut changes = Vec::new();
    for (page, src) in TARGETS {
        let path = site.join(page);
        let html = fs::read_to_string(&path)?;
        let parent = path
            .parent()
            .ok_or_else(|| io::Error::other("missing page parent"))?;
        if parent.join(src).try_exists()? || !html.contains(src) {
            continue;
        }
        let tag = format!("<script src=\"{src}\" async></script>");
        if html.matches(&tag).count() != 1
            || html.matches("<div id=\"implementors-list\"></div>").count() != 1
        {
            return Err(io::Error::other(format!(
                "unexpected implementor markup: {page}"
            )));
        }
        changes.push((path, html.replace(&tag, "")));
    }
    for (path, html) in &changes {
        fs::write(path, html)?;
        println!(
            "Removed empty-implementor script request: {}",
            path.display()
        );
    }
    Ok(changes.len())
}

fn run() -> io::Result<()> {
    let mut args = env::args_os().skip(1);
    let site = args
        .next()
        .ok_or_else(|| io::Error::other("usage: fix-empty-implementors SITE"))?;
    if args.next().is_some() {
        return Err(io::Error::other("usage: fix-empty-implementors SITE"));
    }
    repair(Path::new(&site))?;
    Ok(())
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        path::PathBuf,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use super::*;

    struct Site(PathBuf);

    impl Site {
        fn new() -> Self {
            static NEXT: AtomicUsize = AtomicUsize::new(0);
            let root = env::var_os("TMPDIR").expect("set TMPDIR to a private test directory");
            let site = Self(PathBuf::from(root).join(format!(
                "implementors-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            )));
            fs::create_dir(&site.0).unwrap();
            for (page, src) in TARGETS {
                let path = site.0.join(page);
                fs::create_dir_all(path.parent().unwrap()).unwrap();
                fs::write(path, format!("<div id=\"implementors-list\"></div><script src=\"{src}\" async></script><script src=\"unrelated.js\"></script>")).unwrap();
            }
            site
        }

        fn contents(&self) -> Vec<String> {
            TARGETS
                .iter()
                .map(|(page, _)| fs::read_to_string(self.0.join(page)).unwrap())
                .collect()
        }
    }

    impl Drop for Site {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }

    #[test]
    fn missing_empty_assets_and_idempotence() {
        let site = Site::new();
        assert_eq!(repair(&site.0).unwrap(), 2);
        assert_eq!(repair(&site.0).unwrap(), 0);
        for ((_, src), html) in TARGETS.iter().zip(site.contents()) {
            assert!(!html.contains(src));
            assert!(html.contains("src=\"unrelated.js\""));
        }
    }

    #[test]
    fn real_assets_retained_even_with_implementors() {
        let site = Site::new();
        for (page, src) in TARGETS {
            let path = site.0.join(page);
            let asset = path.parent().unwrap().join(src);
            fs::create_dir_all(asset.parent().unwrap()).unwrap();
            fs::write(asset, "window.register_implementors({});").unwrap();
            fs::write(
                &path,
                fs::read_to_string(&path)
                    .unwrap()
                    .replace("</div>", "<section>impl</section></div>"),
            )
            .unwrap();
        }
        let before = site.contents();
        assert_eq!(repair(&site.0).unwrap(), 0);
        assert_eq!(before, site.contents());
    }

    #[test]
    fn unexpected_markup_fails_without_partial_changes() {
        for replacement in [
            "<section>impl</section></div>",
            "</div><div id=\"implementors-list\"></div>",
        ] {
            let site = Site::new();
            let path = site.0.join(TARGETS[1].0);
            fs::write(
                &path,
                fs::read_to_string(&path)
                    .unwrap()
                    .replace("</div>", replacement),
            )
            .unwrap();
            let before = site.contents();
            assert!(repair(&site.0).is_err());
            assert_eq!(before, site.contents());
        }
    }

    #[test]
    fn changed_script_tag_fails_without_partial_changes() {
        let site = Site::new();
        let path = site.0.join(TARGETS[1].0);
        fs::write(
            &path,
            fs::read_to_string(&path)
                .unwrap()
                .replace(" async>", " defer>"),
        )
        .unwrap();
        let before = site.contents();
        assert!(repair(&site.0).is_err());
        assert_eq!(before, site.contents());
    }

    #[test]
    fn missing_page_fails_without_partial_changes() {
        let site = Site::new();
        let first = site.0.join(TARGETS[0].0);
        let before = fs::read(&first).unwrap();
        fs::remove_file(site.0.join(TARGETS[1].0)).unwrap();
        assert!(repair(&site.0).is_err());
        assert_eq!(before, fs::read(first).unwrap());
    }
}
