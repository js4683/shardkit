"""Check the local Markdown file-link and whitespace contracts; no network access."""

from pathlib import Path
import re
import sys
from urllib.parse import unquote, urlsplit


def check_file(path):
    errors = []
    content = path.read_text(encoding="utf-8")
    for number, line in enumerate(content.splitlines(), 1):
        if line.rstrip() != line:
            errors.append(f"{path}:{number}: trailing whitespace")
    for destination in re.findall(r"\]\(([^\s)]+)\)", content):
        url = urlsplit(destination)
        if url.scheme or url.netloc or not url.path:
            continue
        target = path.parent / unquote(url.path)
        if not target.exists():
            errors.append(f"{path}: missing local link: {destination}")
    if not content.endswith("\n"):
        errors.append(f"{path}: missing final newline")
    return errors


def main():
    root = Path(__file__).resolve().parent.parent
    paths = sorted(root.rglob("*.md"))
    errors = [error for path in paths for error in check_file(path)]
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print(f"Checked {len(paths)} Markdown files: local file links and whitespace pass.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
