# globus-workflow-toolkit

A collection of production scripts, workflows, and tutorials for implementing and automating [Globus](https://www.globus.org/) data transfers. Built for researchers and research computing staff at Purdue and beyond.

---

## Overview

This repository is designed to lower the barrier to adopting Globus for automated, reliable data management. It covers the basics of headless transfers using confidential clients, domain-specific workflow examples, and walkthroughs for setting up Globus features including guest collections, Flows, the CLI, and Globus Compute.

---

## Repository Structure

```
globus-workflow-toolkit/
├── core/          # Foundational transfer scripts
├── tutorials/     # Markdown walkthroughs and setup guides
└── workflows/     # Domain-specific workflow scripts
```

### `core/`
Foundational scripts for common transfer patterns. Includes examples like simple transfers with a lookback window - useful as a starting point for building more complex automation.

### `tutorials/`
Step-by-step guides covering Globus setup and features:
- Setting up guest collections
- Globus CLI usage
- Flows
- Globus Compute
- *(More to come)*

### `workflows/`
Domain-specific workflow scripts built on top of the core transfer logic. Currently includes:
- **Metabolomics** - scans for `.d` folders and automates their transfer

---

## Getting Started

### Prerequisites

- Python 3.8+
- [`globus-sdk`](https://globus-sdk-python.readthedocs.io/en/stable/)
- A Globus account and, for automation, a **confidential client** (see below)

### Installation

```bash
git clone https://github.com/rwilfong/globus-workflow-toolkit.git
cd globus-workflow-toolkit
pip install globus-sdk python-dotenv
```

---

## Confidential Clients

For automation, Globus confidential clients are strongly recommended over native clients. A native client requires manual input to confirm access, which makes headless or scheduled execution impossible. A confidential client uses a secret tied to a client ID (a UUID Globus assigns to your application), allowing scripts to authenticate and run without user interaction.

The Globus [Developer Guide](https://docs.globus.org/api/auth/developer-guide/) covers the full range of application types. This repository focuses on the confidential client pattern.

### Storing Secrets

There are two main approaches used in this repository:

**`.env` file**

An `.env` file stores your credentials alongside the script. This is the recommended approach for scheduled tasks (cron jobs, Windows Task Scheduler) because these jobs typically run in a headless, non-interactive state. On Windows, this is Session 0 - a background state where the OS credential manager is usually locked, making keyring-based approaches unreliable.

```
CLIENT_ID=your-client-id
CLIENT_SECRET=your-client-secret
```

> Never commit your `.env` file. Add it to `.gitignore` and use the provided `.env.example` as a template.

**Python `keyring` library**
The [`keyring`](https://pypi.org/project/keyring/) library stores secrets in your operating system's native credential manager (Windows Credential Locker, macOS Keychain, Linux Secret Service). These are encrypted at rest and a good option for interactive use on a personal or shared workstation.

---

## Contributing

Contributions are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on adding scripts, tutorials, or domain workflows.

---

## About

Developed by Purdue University's Rosen Center for Advanced Computing (RCAC). The goal of this repository is to provide researchers and research computing staff with practical, reusable tooling for standardizing data transfer and management workflows using Globus.

---

## Resources

- [Globus Documentation](https://docs.globus.org/)
- [Globus SDK for Python](https://globus-sdk-python.readthedocs.io/en/stable/)
- [Globus Developer Guide](https://docs.globus.org/api/auth/developer-guide/)
- [Purdue RCAC](https://www.rcac.purdue.edu/)