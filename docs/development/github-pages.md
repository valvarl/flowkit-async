# GitHub Pages Deployment Guide

This page explains reliable ways to publish the documentation site to **GitHub Pages** for the
`valvarl/flowkit-async` project. It uses **MkDocs** (Material theme) and the official
**actions/deploy-pages** workflow.

> If you are working in a fork, replace `valvarl` and `flowkit-async` with your own
> GitHub user/org and repository name.

---

## 1) Automatic deployment with GitHub Actions (recommended)

The workflow below builds the site with MkDocs and publishes it to GitHub Pages on each
push to the default branch. It also supports manual runs via **workflow_dispatch**.

### One‑time repo settings

1. Open **Settings → Pages**.
2. Under **Build and deployment**, set **Source = GitHub Actions**.

### Add workflow: `.github/workflows/docs.yml`

```yaml
name: Deploy Documentation

on:
  push:
    branches: [ main, master ]
    paths:
      - "docs/**"
      - "mkdocs.y*ml"
      - "src/**"
      - "pyproject.toml"
      - "requirements*.txt"
  workflow_dispatch: {}

permissions:
  contents: read
  pages: write
  id-token: write

concurrency:
  group: "pages"
  cancel-in-progress: true

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.12"

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          # Install your package (if your docs import it) and doc toolchain
          pip install -e .
          pip install mkdocs mkdocs-material mkdocstrings[python]

      - name: Build with MkDocs
        run: |
          mkdocs build --clean --strict

      - name: Configure Pages
        uses: actions/configure-pages@v5

      - name: Upload artifact
        uses: actions/upload-pages-artifact@v3
        with:
          path: ./site

  deploy:
    environment:
      name: github-pages
      url: ${{ steps.deployment.outputs.page_url }}
    needs: build
    runs-on: ubuntu-latest
    steps:
      - name: Deploy to GitHub Pages
        id: deployment
        uses: actions/deploy-pages@v4
```

After the first successful run, your docs will be available at:

```
https://valvarl.github.io/flowkit-async/
```

> Tip: If your default branch is `develop` or `main` is protected, adjust
> the `on.push.branches` list accordingly.

---

## 2) Manual deployment (alternative)

If you prefer to push directly to the `gh-pages` branch without Actions:

### Prerequisites

```bash
pip install mkdocs mkdocs-material mkdocstrings[python]
```

### Deploy

```bash
# Build and deploy to gh-pages (will create the branch if missing)
mkdocs gh-deploy

# With a custom message
mkdocs gh-deploy -m "Update documentation"

# Force re-deploy the current build (use with care)
mkdocs gh-deploy --force
```

In **Settings → Pages**, set **Source = Deploy from a branch** and choose `gh-pages` / `/ (root)`.

> You should not use both the GitHub Actions deployment and `gh-deploy` at the same time.
> Pick one strategy to avoid confusion.

---

## 3) Local development

Use MkDocs’ live-reload server while writing docs:

```bash
# Start dev server on :8000
mkdocs serve

# Custom host/port
mkdocs serve --dev-addr=0.0.0.0:8080

# Watch extra paths (e.g., your package code)
mkdocs serve --watch src --watch docs
```

The site will be available at **http://localhost:8000** and will reload on changes.

Build locally the same way Actions does:

```bash
mkdocs build --clean --strict
```

---

## 4) MkDocs configuration template

Edit `mkdocs.yml` (or `mkdocs.yaml`) as needed. A minimal example aligned with this repository:

```yaml
site_name: FlowKit Async
site_url: https://valvarl.github.io/flowkit-async
repo_url: https://github.com/valvarl/flowkit-async
repo_name: valvarl/flowkit-async

theme:
  name: material
  features:
    - navigation.tracking
    - navigation.expand
    - content.code.copy

nav:
  - Home: index.md
  - Getting Started:
      - Quickstart: getting-started/quickstart.md
  - Reference:
      - Core: reference/core.md

markdown_extensions:
  - admonition
  - toc:
      permalink: true
  - tables
  - codehilite

plugins:
  - search
  - mkdocstrings:
      default_handler: python
```

> Keep `site_url` in sync with your actual Pages URL. For forks, replace the owner/repo.

### Custom domain (optional)

1. Add a `CNAME` file **inside the `docs/` directory** so MkDocs copies it into `site/`:
   ```bash
   echo "docs.yourdomain.com" > docs/CNAME
   ```
2. Create a DNS **CNAME** record pointing `docs.yourdomain.com` → `valvarl.github.io` (or your username).
3. Set `site_url` in `mkdocs.yml`:
   ```yaml
   site_url: https://docs.yourdomain.com
   ```

---

## 5) Troubleshooting

**Pages shows 404 or outdated content**
- Make sure **Settings → Pages → Source** is set to **GitHub Actions** (for the workflow above) or `gh-pages` branch (for manual `gh-deploy`).
- Check the latest workflow run logs; verify that `upload-pages-artifact` and `deploy-pages` completed successfully.
- Confirm that `mkdocs build` produced `site/` with your expected pages.

**Build fails in Actions**
- Reproduce locally with:
  ```bash
  mkdocs build --clean --strict
  ```
- Ensure all doc dependencies are installed; if your docstrings import your package, install it with `pip install -e .`.
- Validate configuration:
  ```bash
  mkdocs config
  ```

**Permission errors when deploying**
- The workflow must request:
  ```yaml
  permissions:
    contents: read
    pages: write
    id-token: write
  ```
- Also verify repo settings allow GitHub Pages for this branch/workflow.

**Link or image issues**
- Use relative links within `docs/` (MkDocs resolves them during build).
- Place images under `docs/images/` and reference them relatively: `![Alt](images/diagram.png)`.

---

## 6) Best practices

1. **Preview locally** before pushing:
   ```bash
   mkdocs serve
   ```
2. Run strict builds in CI to catch warnings as errors:
   ```bash
   mkdocs build --strict
   ```
3. Keep docs changes small and focused; review them in PRs.
4. Avoid secrets in docs and examples; prefer environment variables where needed.
5. Optimize images (lossless) to keep the site fast.

---

## 7) URL recap

- Site root: `https://valvarl.github.io/flowkit-async/`
- If you add sections like “Getting Started” or “Reference”, they will appear under that root according to your `nav` in `mkdocs.yml`.
