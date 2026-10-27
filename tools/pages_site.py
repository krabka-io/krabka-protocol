#!/usr/bin/env python3
"""Stage rustdoc for Pages, allowing only linked hidden source HTML.

No build cache is copied: .cache is a path segment in rustdoc's rendered
source tree. All other dotfiles remain excluded. Verify the upload action's
actual archive before allowing the dependent deploy job to run.
"""

import argparse
from html.parser import HTMLParser
from pathlib import Path, PurePosixPath
import posixpath
import shutil
import tarfile
from urllib.parse import unquote, urlsplit


class SourceLinks(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links = []

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "a" and "src" in attrs.get("class", "").split():
            if attrs.get("href"):
                self.links.append(attrs["href"])


def hidden(path):
    return any(part.startswith(".") for part in path.parts)


def permitted_source(path):
    parts = path.parts
    # Only the runner path observed inside rustdoc's source HTML tree.
    return (
        str(path).endswith(".rs.html")
        and [p for p in parts if p.startswith(".")] == [".cache"]
        and any(
            parts[i] == "src"
            and parts[i + 2 : i + 7] == ("home", "runner", ".cache", "bazel", "_bazel_runner")
            for i in range(len(parts) - 6)
        )
    )


def files(root):
    result = set()
    for path in root.rglob("*"):
        if path.is_symlink():
            raise ValueError(f"Symlink in documentation: {path}")
        if path.is_file():
            result.add(path.relative_to(root))
        elif not path.is_dir():
            raise ValueError(f"Not a regular documentation file: {path}")
    return result


def select(root):
    available = files(root)
    selected = {path for path in available if not hidden(path)}
    pending = [path for path in selected if path.suffix == ".html"]
    count = 0
    while pending:
        page = pending.pop()
        parser = SourceLinks()
        parser.feed((root / page).read_text(encoding="utf-8"))
        for href in parser.links:
            url = urlsplit(href)
            if url.scheme or url.netloc:
                continue
            decoded = unquote(url.path)
            target = Path(posixpath.normpath(
                decoded.lstrip("/") if decoded.startswith("/")
                else posixpath.join(page.parent.as_posix(), decoded)
            )) if decoded else page
            if ".." in target.parts or target.is_absolute():
                raise ValueError(f"Source link escapes site: {page}: {href}")
            if hidden(target) and not permitted_source(target):
                raise ValueError(f"Unapproved hidden Source target: {page}: {href}")
            if target not in available:
                raise ValueError(f"Missing Source target: {page}: {href}")
            count += 1
            if target not in selected:
                selected.add(target)
                pending.append(target)
    if not count:
        raise ValueError("No local rustdoc Source links found")
    return selected, count


def stage(source, destination):
    source = source.resolve()
    destination = destination.resolve()
    if source == destination or source in destination.parents or destination in source.parents:
        raise ValueError("Staging and input directories must be separate")
    selected, count = select(source)
    destination.mkdir(parents=True, exist_ok=False)
    for relative in sorted(selected):
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source / relative, target)
    print(f"Staged {len(selected)} files; verified {count} local Source links; "
          f"allowed {sum(hidden(p) for p in selected)} linked hidden source pages")


def verify_archive(site, archive):
    expected, count = select(site)
    if expected != files(site):
        raise ValueError("Staging contains unapproved hidden files")
    actual = set()
    with tarfile.open(archive) as package:
        for member in package:
            path = PurePosixPath(member.name)
            if path.is_absolute() or ".." in path.parts:
                raise ValueError(f"Unsafe archive path: {member.name}")
            if member.isdir():
                continue
            if not member.isfile():
                raise ValueError(f"Non-regular archive entry: {member.name}")
            relative = Path(*path.parts)
            if relative in actual:
                raise ValueError(f"Duplicate archive entry: {member.name}")
            actual.add(relative)
            if relative not in expected:
                raise ValueError(f"Unexpected archived file: {member.name}")
            with package.extractfile(member) as content:
                if content.read() != (site / relative).read_bytes():
                    raise ValueError(f"Archive content mismatch: {member.name}")
    if actual != expected:
        raise ValueError(f"Archive missing {len(expected - actual)} staged files")
    print(f"Verified archive: {len(actual)} files, {count} local Source links")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("stage", "verify"))
    parser.add_argument("site", type=Path)
    parser.add_argument("destination", type=Path)
    args = parser.parse_args()
    try:
        if args.operation == "stage":
            stage(args.site, args.destination)
        else:
            verify_archive(args.site, args.destination)
    except (ValueError, OSError, tarfile.TarError) as error:
        parser.exit(1, f"{error}\n")
