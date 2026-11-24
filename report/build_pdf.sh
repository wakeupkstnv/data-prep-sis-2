#!/usr/bin/env bash
# Rebuild report/project_report.pdf from project_report.md (needs pandoc + Google Chrome).
set -euo pipefail
cd "$(dirname "$0")"
pandoc project_report.md -f markdown -t html5 -s --toc --toc-depth=2 \
  --metadata pagetitle="SIS2 Project Report" --metadata toc-title="Contents" \
  --css report.css --embed-resources -o project_report.html
# move the table of contents after the title page
python3 - <<'PY'
import re
html = open("project_report.html", encoding="utf-8").read()
toc = re.search(r'<nav id="TOC".*?</nav>', html, re.S).group(0)
html = html.replace(toc, "", 1)
i = html.index('<h1 id="introduction"')
html = html[:i] + toc + "\n" + html[i:]
open("project_report.html", "w", encoding="utf-8").write(html)
PY
CHROME="${CHROME:-/Applications/Google Chrome.app/Contents/MacOS/Google Chrome}"
"$CHROME" --headless=new --disable-gpu --no-pdf-header-footer \
  --print-to-pdf="$PWD/project_report.pdf" "file://$PWD/project_report.html" 2>/dev/null
rm -f project_report.html
echo "built $PWD/project_report.pdf"
