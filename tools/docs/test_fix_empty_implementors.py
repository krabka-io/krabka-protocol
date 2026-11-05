"""Exercise missing/real assets, nonempty lists, scope, and repeat execution."""

import tempfile
import unittest
from pathlib import Path

from fix_empty_implementors import TARGETS, repair


class RepairTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.site = Path(self.tmp.name)
        for page, src in TARGETS:
            path = self.site / page
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text('<div id="implementors-list"></div>'
                            f'<script src="{src}" async></script>'
                            '<script src="unrelated.js"></script>')

    def test_missing_empty_assets_and_idempotence(self):
        self.assertEqual(repair(self.site), 2)
        self.assertEqual(repair(self.site), 0)
        for page, src in TARGETS:
            html = (self.site / page).read_text()
            self.assertNotIn(src, html)
            self.assertIn('src="unrelated.js"', html)

    def test_real_asset_retained_even_with_implementors(self):
        for page, src in TARGETS:
            path = self.site / page
            asset = path.parent / src
            asset.parent.mkdir(parents=True, exist_ok=True)
            asset.write_text('window.register_implementors({});')
            path.write_text(path.read_text().replace('</div>', '<section>impl</section></div>'))
        before = [(self.site / page).read_bytes() for page, _ in TARGETS]
        self.assertEqual(repair(self.site), 0)
        self.assertEqual(before, [(self.site / page).read_bytes() for page, _ in TARGETS])

    def test_nonempty_list_fails_without_partial_changes(self):
        path = self.site / TARGETS[1][0]
        path.write_text(path.read_text().replace('</div>', '<section>impl</section></div>'))
        before = [(self.site / page).read_bytes() for page, _ in TARGETS]
        with self.assertRaises(ValueError):
            repair(self.site)
        self.assertEqual(before, [(self.site / page).read_bytes() for page, _ in TARGETS])


if __name__ == "__main__":
    unittest.main()
