#!/usr/bin/env python3
import argparse
import html
import random
import re
import shutil
import sys
import time
import urllib.request
from pathlib import Path

USER_AGENT = "Mozilla/5.0 (compatible; Lab2HtmlDatasetPreparer/1.0)"

SNIPPETS = [
    '<div class="card"><h2>Products</h2><p>Fresh offers every day.</p><a href="/shop">Open shop</a></div>',
    '<section><article><h3>News</h3><p>Breaking updates for document processing.</p></article></section>',
    '<ul><li>Item A</li><li>Item B</li><li>Item C</li></ul>',
    '<table><tr><td>Q1</td><td>120</td></tr><tr><td>Q2</td><td>140</td></tr></table>',
    '<form><input type="text" value="sample"><button>Send</button></form>',
    '<nav><a href="/home">Home</a><a href="/about">About</a><a href="/contact">Contact</a></nav>',
    '<footer><p>Generated for parallel programming lab.</p></footer>',
    '<header><h1>Sample Page</h1><span>metadata</span></header>',
    '<main><section><p>Lorem ipsum dolor sit amet.</p></section></main>',
    '<article><img src="image.png" alt="img"><p>Image block</p></article>',
]

TITLE_WORDS = [
    "Parallel", "Worker", "Fork", "Join", "Pipeline", "Dataset",
    "Benchmark", "HTML", "Thread", "Pattern", "Task", "Sample"
]

BODY_CLASSES = ["page", "content", "layout", "wrapper", "document", "article-body"]
LANGS = ["en", "uk", "de", "fr", "pl"]

URL_FILENAME_SAFE = re.compile(r"[^a-zA-Z0-9._-]+")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Prepare a local HTML dataset for the lab."
    )
    parser.add_argument("--out-dir", default="data/html", help="Output directory for final HTML files")
    parser.add_argument("--count", type=int, default=1000, help="How many HTML files to create")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--clean", action="store_true", help="Delete existing output directory before generation")

    parser.add_argument(
        "--seed-dir",
        default="seed_html",
        help="Directory with local seed HTML files; if present, files will be cloned/mutated"
    )
    parser.add_argument(
        "--urls-file",
        default=None,
        help="Optional text file with URLs to download as seed HTML pages"
    )
    parser.add_argument(
        "--download-dir",
        default="seed_html_downloaded",
        help="Where downloaded HTML seed files are stored"
    )
    return parser.parse_args()


def ensure_clean_dir(path: Path, clean: bool):
    if clean and path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def read_urls_file(urls_file: Path):
    urls = []
    for line in urls_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        urls.append(line)
    return urls


def safe_filename_from_url(url: str, index: int) -> str:
    raw = url.replace("https://", "").replace("http://", "")
    raw = URL_FILENAME_SAFE.sub("_", raw).strip("_")
    if not raw:
        raw = f"url_{index}"
    return f"{index:03d}_{raw[:80]}.html"


def download_seed_pages(urls, download_dir: Path):
    download_dir.mkdir(parents=True, exist_ok=True)
    saved_files = []

    for idx, url in enumerate(urls, start=1):
        filename = safe_filename_from_url(url, idx)
        target = download_dir / filename

        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        try:
            with urllib.request.urlopen(req, timeout=20) as response:
                content_type = response.headers.get("Content-Type", "")
                charset = response.headers.get_content_charset() or "utf-8"
                raw = response.read()

                text = raw.decode(charset, errors="replace")

                if "<html" not in text.lower():
                    text = wrap_non_html_response(url, text, content_type)

                target.write_text(text, encoding="utf-8")
                saved_files.append(target)
                print(f"[downloaded] {url} -> {target}")
                time.sleep(0.3)
        except Exception as e:
            print(f"[warning] failed to download {url}: {e}")

    return saved_files


def wrap_non_html_response(url: str, text: str, content_type: str) -> str:
    escaped = html.escape(text[:20000])
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Wrapped response for {html.escape(url)}</title>
</head>
<body>
  <h1>Wrapped response</h1>
  <p>Original URL: {html.escape(url)}</p>
  <p>Content-Type: {html.escape(content_type)}</p>
  <pre>{escaped}</pre>
</body>
</html>
"""


def load_seed_html_files(seed_dir: Path):
    if not seed_dir.exists():
        return []

    files = sorted(
        [p for p in seed_dir.rglob("*") if p.is_file() and p.suffix.lower() in {".html", ".htm"}]
    )

    docs = []
    for path in files:
        try:
            docs.append(path.read_text(encoding="utf-8", errors="replace"))
        except Exception as e:
            print(f"[warning] failed to read {path}: {e}")
    return docs


def build_template_doc(rng: random.Random, doc_id: int) -> str:
    title = build_random_title(rng, doc_id)
    body_class = rng.choice(BODY_CLASSES)
    lang = rng.choice(LANGS)

    parts = []
    for _ in range(rng.randint(8, 25)):
        parts.append(rng.choice(SNIPPETS))
        if rng.random() < 0.35:
            parts.append(f'<p>Generated paragraph {rng.randint(1, 9999)}</p>')
        if rng.random() < 0.20:
            parts.append(f'<span data-id="{rng.randint(1000, 9999)}">meta</span>')

    body = "\n    ".join(parts)

    return f"""<!DOCTYPE html>
<html lang="{lang}">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{html.escape(title)}</title>
</head>
<body class="{body_class}">
  <header>
    <h1>{html.escape(title)}</h1>
    <nav><a href="/">Home</a><a href="/docs">Docs</a><a href="/contact">Contact</a></nav>
  </header>
  <main>
    {body}
  </main>
  <footer><p>Document #{doc_id}</p></footer>
</body>
</html>
"""


def build_random_title(rng: random.Random, doc_id: int) -> str:
    words = rng.sample(TITLE_WORDS, k=rng.randint(2, 4))
    return f"{' '.join(words)} {doc_id}"


def mutate_html(seed_html: str, rng: random.Random, doc_id: int) -> str:
    text = seed_html

    if "<html" not in text.lower():
        text = wrap_non_html_response(f"seed-{doc_id}", text, "text/plain")

    new_title = build_random_title(rng, doc_id)
    if re.search(r"<title>.*?</title>", text, flags=re.IGNORECASE | re.DOTALL):
        text = re.sub(
            r"<title>.*?</title>",
            f"<title>{html.escape(new_title)}</title>",
            text,
            count=1,
            flags=re.IGNORECASE | re.DOTALL
        )
    else:
        text = text.replace(
            "<head>",
            "<head>\n  <title>{}</title>".format(html.escape(new_title)),
            1
        )

    extra_count = rng.randint(2, 8)
    extra_blocks = []
    for _ in range(extra_count):
        extra_blocks.append(rng.choice(SNIPPETS))
        if rng.random() < 0.5:
            extra_blocks.append(f'<div class="generated"><p>doc={doc_id} value={rng.randint(1, 99999)}</p></div>')
    injection = "\n".join(extra_blocks)

    if re.search(r"</body\s*>", text, flags=re.IGNORECASE):
        text = re.sub(r"</body\s*>", injection + "\n</body>", text, count=1, flags=re.IGNORECASE)
    else:
        text += "\n<body>\n" + injection + "\n</body>\n"

    if "<!doctype" not in text.lower():
        text = "<!DOCTYPE html>\n" + text

    return text


def write_dataset(out_dir: Path, docs, count: int, rng: random.Random):
    out_dir.mkdir(parents=True, exist_ok=True)

    use_seeds = bool(docs)
    created = 0

    for i in range(1, count + 1):
        if use_seeds:
            seed_html = rng.choice(docs)
            final_html = mutate_html(seed_html, rng, i)
        else:
            final_html = build_template_doc(rng, i)

        target = out_dir / f"doc_{i:04d}.html"
        target.write_text(final_html, encoding="utf-8")
        created += 1

    return created


def main():
    args = parse_args()
    rng = random.Random(args.seed)

    out_dir = Path(args.out_dir)
    seed_dir = Path(args.seed_dir)
    download_dir = Path(args.download_dir)

    ensure_clean_dir(out_dir, args.clean)

    if args.urls_file:
        urls_file = Path(args.urls_file)
        if not urls_file.exists():
            print(f"[error] URLs file not found: {urls_file}")
            sys.exit(1)

        urls = read_urls_file(urls_file)
        if not urls:
            print("[error] URLs file is empty")
            sys.exit(1)

        download_seed_pages(urls, download_dir)

    seed_docs = load_seed_html_files(seed_dir) + load_seed_html_files(download_dir)

    created = write_dataset(out_dir, seed_docs, args.count, rng)

    print()
    print("Dataset prepared successfully.")
    print(f"Output directory: {out_dir.resolve()}")
    print(f"HTML files created: {created}")
    print(f"Seed documents used: {len(seed_docs)}")
    print("Mode:", "seed clone / mutation" if seed_docs else "template generation")


if __name__ == "__main__":
    main()
