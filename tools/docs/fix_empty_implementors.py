"""Remove two verified rustdoc requests for unwritten empty-implementor assets.

Temporary output workaround: rustdoc emits these script tags even when no
implementors exist. Never invent an asset or remove a request for a real asset.
Keep this allowlist narrow; unexpected generator changes must be investigated.
"""

import argparse
from pathlib import Path


TARGETS = (
    ("krabka-security/krabka_security/trait.IntrospectionClient.html",
     "../trait.impl/krabka_security/oauthbearer/trait.IntrospectionClient.js"),
    ("krabka-metadata/krabka_metadata/trait.Feature.html",
     "../trait.impl/krabka_metadata/feature/trait.Feature.js"),
)


def repair(site):
    changes = []
    for page, src in TARGETS:
        path = site / page
        html = path.read_text(encoding="utf-8")
        tag = f'<script src="{src}" async></script>'
        if (path.parent / src).exists() or src not in html:
            continue
        # Require the exact known empty list and generated tag. Do not strip
        # scripts from populated lists or silently accept changed HTML markup.
        if html.count(tag) != 1 or html.count('<div id="implementors-list"></div>') != 1:
            raise ValueError(f"unexpected implementor markup: {page}")
        changes.append((path, html.replace(tag, "")))
    # Validate both pages before changing either one.
    for path, html in changes:
        path.write_text(html, encoding="utf-8")
        print(f"Removed empty-implementor script request: {path}")
    return len(changes)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("site", type=Path)
    repair(parser.parse_args().site)
