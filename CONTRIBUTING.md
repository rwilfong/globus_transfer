# Contributing to globus-workflow-toolkit

Thank you for your interest in contributing! This repository is designed to grow into a shared resource for researchers and research computing staff implementing Globus workflows. 

Contributions of all kinds are welcome - whether that's a new script, a walkthrough, a bug fix, or a documentation improvement.

---

## Repository Structure

```
globus-workflow-toolkit/
├── core/          # Foundational transfer scripts (e.g., simple transfers with lookback windows)
├── tutorials/     # Markdown walkthroughs for setup and Globus features
└── workflows/     # Domain-specific workflow scripts (e.g., metabolomics .d folder handling)
```

When adding new content, place it in the most appropriate folder. If your contribution doesn't fit neatly into one, open an issue first to discuss where it belongs.

---

## How to Contribute

### 1. Open an Issue First (for significant changes)

Before writing code or a tutorial, open an issue to describe what you'd like to add or change. This prevents duplicated effort and allows for early feedback.

Use the following issue types as a guide:
- **Bug report** - something is broken or behaving unexpectedly
- **Feature request** - a new script, workflow, or tutorial you'd like to see
- **Documentation** - improvements to existing walkthroughs or READMEs

### 2. Fork and Clone

```bash
git clone https://github.com/rwilfong/globus-workflow-toolkit.git
cd globus-workflow-toolkit
```

### 3. Create a Branch

Use a descriptive branch name:

```bash
git checkout -b feature/add-flows-tutorial
git checkout -b fix/metabolomics-path-handling
git checkout -b docs/update-guest-collection-walkthrough
```

### 4. Make Your Changes

Keep commits focused and descriptive. A few good commits are better than one large one.

```bash
git commit -m "Add basic Globus Flows trigger tutorial"
```

### 5. Open a Pull Request

Push your branch and open a pull request against `main`. In your PR description:
- Reference the issue it addresses (e.g., `Closes #12`)
- Briefly describe what changed and why
- Note any testing you did

---

## Contribution Guidelines

### Scripts (`core/` and `workflows/`)

- Write in Python unless there is a strong reason not to
- Include a docstring or header comment explaining what the script does, its inputs, and any required environment setup
- Do not hardcode credentials - use `.env` files or the `keyring` library (see README for guidance)
- Never commit `.env` files, secrets, client IDs, or UUIDs - add them to `.gitignore`
- Include a sample `.env.example` if your script requires one

### Tutorials (`tutorials/`)

- Write in Markdown
- Include a brief intro explaining what the tutorial covers and who it is for
- Use code blocks for all commands and configuration snippets
- Link to relevant Globus documentation where appropriate
- Keep steps numbered and sequential

### Domain Workflows (`workflows/`)

- Include a README in your workflow subdirectory describing the use case, inputs, and expected outputs
- Note any domain-specific dependencies or assumptions

---

## Code Style

- Follow [PEP 8](https://peps.python.org/pep-0008/) for Python
- Use meaningful variable names - clarity over brevity
- Avoid unnecessary dependencies; prefer the standard library and `globus-sdk` where possible

---

## Questions

If you're unsure about anything, open an issue or reach out. This project is meant to be useful to the broader research computing community, and good-faith contributions are always welcome.