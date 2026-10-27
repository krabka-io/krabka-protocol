"""Behavioral packaging regressions; temporary files stay in the workspace."""

from pathlib import Path
import subprocess
import shutil
import tarfile
import tempfile
import unittest

from pages_site import stage, verify_archive


class PagesSiteTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(dir=Path(__file__).parent)
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.site = self.root / "site"
        self.output = self.root / "staged"
        self.archive = self.root / "artifact.tar"
        self.target = (
            "protocol/src/krabka_protocol/home/runner/.cache/bazel/_bazel_runner/"
            "hash/sandbox/processwrapper-sandbox/1/execroot/_main/"
            "crates/protocol/generated/Message.owned.rs.html"
        )
        self.write(self.target, "<pre>generated source</pre>")
        self.write("protocol/krabka_protocol/struct.Message.html",
                   f'<a class="src rightside" href="../{self.target.removeprefix("protocol/")}#1-4">Source</a>')
        self.write("index.html", "API docs")
        self.write("static.files/main.js", "// asset")

    def write(self, name, content):
        path = self.site / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)

    def package(self, exclude_hidden=False):
        command = ["tar", "--dereference", "--hard-dereference", "--directory",
                   str(self.output), "-cf", str(self.archive),
                   "--exclude=.git", "--exclude=.github"]
        if exclude_hidden:
            command.append("--exclude=.[^/]*")
        subprocess.run([*command, "."], check=True)

    def test_preserves_only_linked_hidden_source_and_final_archive(self):
        for secret in (".env", ".git/config", ".github/token", "protocol/.env",
                       self.target + ".env", self.target.replace("Message", "Unlinked"),
                       self.target.replace("Message.owned.rs.html", ".token")):
            self.write(secret, "not for publication")
        stage(self.site, self.output)
        expected = {"index.html", "static.files/main.js",
                    "protocol/krabka_protocol/struct.Message.html", self.target}
        self.assertEqual({str(p.relative_to(self.output)) for p in self.output.rglob("*")
                          if p.is_file()}, expected)
        self.package()
        verify_archive(self.output, self.archive)

    def test_catches_original_hidden_exclusion_in_final_archive(self):
        stage(self.site, self.output)
        self.package(exclude_hidden=True)
        with self.assertRaisesRegex(ValueError, "Archive missing"):
            verify_archive(self.output, self.archive)

    def test_missing_source_fails_before_staging(self):
        (self.site / self.target).unlink()
        with self.assertRaisesRegex(ValueError, "Missing Source"):
            stage(self.site, self.output)
        self.assertFalse(self.output.exists())

    def test_symlink_is_rejected(self):
        outside = self.root / "private"
        outside.write_text("secret")
        (self.site / "leak.html").symlink_to(outside)
        with self.assertRaisesRegex(ValueError, "Symlink"):
            stage(self.site, self.output)

    def test_unapproved_hidden_and_escaping_links_are_rejected(self):
        for href in ("../../.env", "../../../private", "../../%2eenv",
                     "../../src/not-source/.cache/secrets.html"):
            with self.subTest(href=href):
                self.write("protocol/krabka_protocol/struct.Message.html",
                           f'<a class="src" href="{href}">Source</a>')
                with self.assertRaises(ValueError):
                    stage(self.site, self.output)

    def test_post_staging_hidden_injection_is_rejected(self):
        stage(self.site, self.output)
        (self.output / ".env").write_text("secret")
        self.package()
        with self.assertRaisesRegex(ValueError, "unapproved hidden"):
            verify_archive(self.output, self.archive)

    def test_unexpected_archive_entry_is_rejected(self):
        stage(self.site, self.output)
        self.package()
        unexpected = self.root / "unexpected"
        unexpected.write_text("secret")
        with tarfile.open(self.archive, "a") as package:
            package.add(unexpected, arcname=".env")
        with self.assertRaisesRegex(ValueError, "Unexpected archived"):
            verify_archive(self.output, self.archive)

    def test_no_source_links_fails_closed(self):
        self.write("protocol/krabka_protocol/struct.Message.html", "no sources")
        with self.assertRaisesRegex(ValueError, "No local rustdoc Source"):
            stage(self.site, self.output)

    @unittest.skipUnless(shutil.which("rustdoc"), "rustdoc not installed")
    def test_real_rustdoc_source_pages_survive_packaging(self):
        generated = self.root / "home/runner/.cache/bazel/_bazel_runner/hash/generated/Message.rs"
        generated.parent.mkdir(parents=True)
        generated.write_text("/// A generated message.\npub struct Message;\n")
        (self.root / "lib.rs").write_text(
            'include!("home/runner/.cache/bazel/_bazel_runner/hash/generated/Message.rs");\n'
        )
        raw = self.root / "rustdoc"
        subprocess.run(["rustdoc", "--crate-name", "repro", "lib.rs", "-o", str(raw)],
                       cwd=self.root, check=True)
        stage(raw, self.output)
        self.assertTrue(any(".cache" in p.parts for p in self.output.rglob("*.rs.html")))
        self.package()
        verify_archive(self.output, self.archive)


if __name__ == "__main__":
    unittest.main()
