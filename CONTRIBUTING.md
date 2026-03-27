# Contributing to GPDB

First off, thank you for considering contributing. Every contribution helps to improve the project.

## Contribution Workflow

We follow a standard GitHub fork-and-pull workflow:

1.  **Fork the repository** to your own GitHub account.
2.  **Clone your fork** to your local machine: `git clone https://github.com/YourUsername/gpdb.git`
3.  **Create a new branch** for your changes: `git checkout -b <type>/<topic>` (See Branch Naming below).
4.  **Make your changes**, ensuring you follow the guidelines below.
5.  **Commit your changes** using the Conventional Commits format.
6.  **Push your branch** to your fork: `git push origin <type>/<topic>`
7.  **Submit a Pull Request** from your feature branch to the main GPDB repository.

## Branch Naming Standards

To keep the repository organized, all branches must follow the `<type>/<short-description>` format:

| Type | Purpose | Example |
| :--- | :--- | :--- |
| `feat/` | A new feature or capability. | `feat/bloom-filters` |
| `fix/` | A bug fix. | `fix/manifest-corruption` |
| `refactor/` | Code change that neither fixes a bug nor adds a feature. | `refactor/io-layer` |
| `docs/` | Documentation changes only. | `docs/api-examples` |
| `chore/` | Maintenance, dependencies, or CI/CD updates. | `chore/update-serde` |
| `test/` | Adding or correcting tests. | `test/wal-recovery` |

## Commit Message Guidelines

All commit messages must follow the [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) specification. 

**Format:**
```
<type>(<scope>): <subject>
```

**Example:**
`feat(sstable): implement sequential iterator`

## Pull Request Standards

To ensure a smooth workflow, **Pull Request titles must match the branch naming type and topic.**

**Format:**
`<type>/<topic>`

**Example:**
If your branch is `feat/background-compaction`, your PR title should be `feat/background-compaction`.

## Code Style

*   **Format**: Run `cargo fmt` before committing to ensure consistent code style.
*   **Comments**: Focus on the **Why**, not the **What**. Public API members should always be documented.

Thank you for helping build a better storage engine!
