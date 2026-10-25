"""Converts generated Markdown reports into clean, standalone, styled HTML reports.

Features:
- GitHub/RWS-like typography and clean modern styling (Rijkswaterstaat navy blue accents)
- Responsive container with styled tables, alternating rows, sticky headers
- KaTeX CDN integration for inline and display LaTeX math ($...$ and $$...$$)
- Admonition/alert callout styling (e.g. > [!NOTE], > [!TIP])
"""

import re
from pathlib import Path
import markdown

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="nl">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>__TITLE__</title>
  <!-- KaTeX for math rendering -->
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/katex@0.16.11/dist/katex.min.css">
  <script defer src="https://cdn.jsdelivr.net/npm/katex@0.16.11/dist/katex.min.js"></script>
  <script defer src="https://cdn.jsdelivr.net/npm/katex@0.16.11/dist/contrib/auto-render.min.js"
          onload="renderMathInElement(document.body, {
            delimiters: [
              {left: '$$', right: '$$', display: true},
              {left: '$', right: '$', display: false}
            ],
            throwOnError: false
          });"></script>
  <style>
    :root {
      --primary: #003366;       /* RWS Dark Blue */
      --secondary: #007bc7;     /* RWS Light Blue */
      --accent: #e6f1f8;
      --text: #24292f;
      --bg: #f8fafc;
      --card-bg: #ffffff;
      --border: #d0d7de;
      --table-stripe: #f6f8fa;
    }
    * {
      box-sizing: border-box;
    }
    body {
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
      color: var(--text);
      background-color: var(--bg);
      line-height: 1.6;
      margin: 0;
      padding: 30px 15px;
    }
    .container {
      max-width: 1250px;
      margin: 0 auto;
      background: var(--card-bg);
      padding: 40px 50px;
      border-radius: 8px;
      box-shadow: 0 4px 12px rgba(0, 0, 0, 0.06);
      border: 1px solid var(--border);
    }
    h1, h2, h3, h4, h5, h6 {
      color: var(--primary);
      margin-top: 1.5em;
      margin-bottom: 0.6em;
      font-weight: 600;
      line-height: 1.25;
    }
    h1 {
      font-size: 2.1rem;
      border-bottom: 2px solid var(--secondary);
      padding-bottom: 0.3em;
      margin-top: 0;
    }
    h2 {
      font-size: 1.45rem;
      border-bottom: 1px solid var(--border);
      padding-bottom: 0.3em;
    }
    h3 {
      font-size: 1.2rem;
    }
    p, ul, ol {
      margin-top: 0;
      margin-bottom: 1em;
    }
    code {
      font-family: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, monospace;
      font-size: 85%;
      background-color: #eff1f3;
      padding: 0.2em 0.4em;
      border-radius: 4px;
    }
    pre code {
      display: block;
      padding: 1em;
      overflow-x: auto;
      background: #24292f;
      color: #f0f6fc;
      border-radius: 6px;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      margin: 1.5em 0;
      font-size: 0.90rem;
      display: block;
      overflow-x: auto;
    }
    th, td {
      padding: 9px 12px;
      border: 1px solid var(--border);
      text-align: left;
    }
    th {
      background-color: var(--primary);
      color: #ffffff;
      font-weight: 600;
      white-space: nowrap;
    }
    tr:nth-child(even) {
      background-color: var(--table-stripe);
    }
    tr:hover {
      background-color: #eef5fb;
    }
    blockquote {
      margin: 1.5em 0;
      padding: 12px 18px;
      color: #444;
      background-color: var(--accent);
      border-left: 5px solid var(--secondary);
      border-radius: 4px;
    }
    blockquote p:last-child {
      margin-bottom: 0;
    }
    hr {
      border: 0;
      height: 1px;
      background: var(--border);
      margin: 2.5em 0;
    }
    footer {
      margin-top: 3em;
      font-size: 0.85rem;
      color: #666;
      text-align: center;
      border-top: 1px solid var(--border);
      padding-top: 1.5em;
    }
  </style>
</head>
<body>
  <div class="container">
    __CONTENT__
    <footer>
      RWS Inland Waterway Network Analysis &mdash; Rapportage
    </footer>
  </div>
</body>
</html>
"""


def convert_markdown_to_html(md_path: Path, html_path: Path | None = None) -> Path:
    if html_path is None:
        html_path = md_path.with_suffix(".html")
    raw_md = md_path.read_text(encoding="utf-8")

    title_match = re.search(r"^#\s+(.+)$", raw_md, re.MULTILINE)
    title = (
        title_match.group(1).strip()
        if title_match
        else md_path.stem.replace("_", " ").title()
    )

    # Pre-process GitHub-style alerts like > [!NOTE]
    processed_md = re.sub(
        r">\s*\[!(NOTE|TIP|IMPORTANT|WARNING|CAUTION)\]\s*", r"> **:** ", raw_md
    )

    body_html = markdown.markdown(
        processed_md,
        extensions=["tables", "fenced_code", "sane_lists"],
    )

    full_html = HTML_TEMPLATE.replace("__TITLE__", title).replace(
        "__CONTENT__", body_html
    )
    html_path.write_text(full_html, encoding="utf-8")
    return html_path


def main():
    reports = [
        Path("output/berth-analysis/berth_spatial_report.md"),
        Path("output/fis_validation_report.md"),
        Path("output/merged_validation_report.md"),
        Path("output/bivas-validation/lock_chamber_consistency_report.md"),
        Path("output/bivas-validation/comparison_report.md"),
        Path("output/route_validation_report.md"),
    ]
    for p in reports:
        if p.exists():
            out = convert_markdown_to_html(p)
            print(f"Rendered: {out} ({out.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
