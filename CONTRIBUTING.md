# Contributing to GPDB

First off, thank you for considering contributing. Every contribution helps to improve the project.

## Contribution Workflow

We follow a standard GitHub fork-and-pull workflow:

1.  **Fork the repository** to your own GitHub account.
2.  **Clone your fork** to your local machine: `git clone https://github.com/YourUsername/GPDB.git`
3.  **Create a new branch** for your changes: `git checkout -b your-feature-name`
4.  **Make your changes**, ensuring you follow the guidelines below.
5.  **Commit your changes** using the commit message format.
6.  **Push your branch** to your fork: `git push origin your-feature-name`
7.  **Submit a Pull Request** from your feature branch to the main GPDB repository.

## Commit Message Guidelines

All commit messages must follow the [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) specification. This helps in maintaining a clear and readable commit history.

The commit message should be structured as follows:

```
<type>(<scope>): <subject>

[optional body]

[optional footer(s)]
```

**Common Types:**

*   `feat`: A new feature.
*   `fix`: A bug fix.
*   `test`: Adding missing tests or correcting existing tests.
*   `chore`: Changes to the build process or auxiliary tools and libraries.
*   `docs`: Documentation only changes.
*   `refactor`: A code change that neither fixes a bug nor adds a feature.
*   `style`: Changes that do not affect the meaning of the code (white-space, formatting, etc).

**Example:**

```
feat(db): Implement memtable flushing to sstables

This commit introduces the core feature of flushing the memtable to
disk as an sstable when it reaches its configured size.
```

## Code Style and Comments

*   **Code Style:** Follow the idiomatic style of the language you are working in (Go or Rust). Run `go fmt` or `cargo fmt` before committing to ensure consistent formatting.
*   **Comments:** Comments should be used sparingly. The code should be as self-documenting as possible.
    *   **DO** add comments to explain the *why* behind a piece of code, especially for complex algorithms, non-obvious logic, or important trade-offs; Always add comments for functions, methods, custom types , and classes, inorder to make them easy to use.
    *   **DO NOT** add comments that simply restate what the code is doing. For example, avoid comments like `// increment i`.

Thank you for your contribution!
