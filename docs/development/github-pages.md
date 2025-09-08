# GitHub Pages Deployment Guide

This guide explains how to deploy FlowKit documentation to GitHub Pages.

## Automatic Deployment (Recommended)

The repository includes a GitHub Actions workflow that automatically builds and deploys documentation when you push to the main/master branch.

### Setup Steps

1. **Enable GitHub Pages** in your repository:
   - Go to repository Settings → Pages
   - Source: "GitHub Actions"
   - Save the settings

2. **The workflow will automatically**:
   - Trigger on push to main/master branch
   - Build the documentation using MkDocs
   - Deploy to GitHub Pages
   - Make docs available at `https://your-username.github.io/flowkit`

### Workflow Configuration

The workflow is defined in `.github/workflows/docs.yml`:

```yaml
name: Deploy Documentation
on:
  push:
    branches: [main, master]
permissions:
  contents: read
  pages: write
  id-token: write
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      - name: Install dependencies
        run: |
          pip install -e .
          pip install mkdocs mkdocs-material mkdocstrings[python]
      - name: Build documentation
        run: mkdocs build --clean --strict
      - name: Upload artifact
        uses: actions/upload-pages-artifact@v3
        with:
          path: ./site
  deploy:
    if: github.ref == 'refs/heads/main'
    environment:
      name: github-pages
      url: ${{ steps.deployment.outputs.page_url }}
    runs-on: ubuntu-latest
    needs: build
    steps:
      - name: Deploy to GitHub Pages
        uses: actions/deploy-pages@v4
```

## Manual Deployment

You can also deploy manually using the MkDocs CLI:

### Prerequisites

```bash
pip install mkdocs mkdocs-material mkdocstrings[python]
```

### Deploy Command

```bash
# Build and deploy to gh-pages branch
mkdocs gh-deploy

# With custom commit message
mkdocs gh-deploy -m "Update documentation"

# Force push (use with caution)
mkdocs gh-deploy --force
```

### Manual Setup for GitHub Pages

If using manual deployment:

1. **Repository Settings**:
   - Go to Settings → Pages
   - Source: "Deploy from a branch"
   - Branch: `gh-pages` / `/ (root)`

2. **Deploy**:
   ```bash
   mkdocs gh-deploy
   ```

## Local Development

### Serve Documentation Locally

```bash
# Start development server
mkdocs serve

# Custom host/port
mkdocs serve --dev-addr=0.0.0.0:8080

# Auto-reload on changes
mkdocs serve --watch=src --watch=docs
```

The docs will be available at `http://localhost:8000` with live reloading.

### Build Documentation

```bash
# Build static site
mkdocs build

# Build with strict mode (fail on warnings)
mkdocs build --strict

# Clean build
mkdocs build --clean
```

## Customization

### Site Configuration

Edit `mkdocs.yml` to customize:

```yaml
site_name: FlowKit
site_url: https://your-org.github.io/flowkit
repo_url: https://github.com/your-org/flowkit

theme:
  name: material
  palette:
    primary: blue
    accent: blue
```

### Custom Domain

To use a custom domain:

1. **Add CNAME file**:
   ```bash
   echo "docs.yourdomain.com" > docs/CNAME
   ```

2. **Configure DNS**:
   - Create a CNAME record pointing to `your-username.github.io`

3. **Update mkdocs.yml**:
   ```yaml
   site_url: https://docs.yourdomain.com
   ```

## Troubleshooting

### Common Issues

**Build Failures**:
```bash
# Check for syntax errors
mkdocs build --strict

# Validate configuration
mkdocs config
```

**Missing Dependencies**:
```bash
# Install all documentation dependencies
pip install mkdocs mkdocs-material mkdocstrings[python]
```

**Import Errors**:
```bash
# Install the package in development mode
pip install -e .
```

**GitHub Actions Permissions**:
- Ensure Pages permissions are enabled in repository settings
- Check that the workflow has `pages: write` permission

### Debug Locally

```bash
# Verbose build output
mkdocs build --verbose

# Check configuration
mkdocs config

# Validate all internal links
mkdocs build --strict
```

## Best Practices

1. **Always test locally** before pushing:
   ```bash
   mkdocs serve
   # Check all pages and links work
   ```

2. **Use strict mode** to catch issues:
   ```bash
   mkdocs build --strict
   ```

3. **Keep documentation in sync** with code changes

4. **Use descriptive commit messages** for documentation changes

5. **Review documentation** in pull requests

## URL Structure

After deployment, your documentation will be available at:

- Main site: `https://your-username.github.io/flowkit/`
- Getting Started: `https://your-username.github.io/flowkit/getting-started/quickstart/`
- API Reference: `https://your-username.github.io/flowkit/reference/core/`
- Examples: `https://your-username.github.io/flowkit/examples/simple-pipeline/`

## Security Considerations

- Documentation is public by default
- Don't include sensitive information in docs
- Use environment variables for secrets in examples
- Review all code examples for security issues

## Performance Optimization

- Use `mkdocs build --clean` for clean builds
- Optimize images in `docs/images/`
- Minimize external dependencies
- Use MkDocs Material's optimization features
