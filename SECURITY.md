# Security Policy

## Reporting a Vulnerability

**Please do NOT open a public issue for security vulnerabilities.**

### Recommended: GitHub Private Vulnerability Reporting

Use [GitHub's built-in private vulnerability reporting](https://github.com/Saxy/Tellstone/security/advisories/new) to report vulnerabilities directly. This keeps the report private between you and the maintainers until a fix is available.

### Alternative: Email

<!-- Replace with a dedicated security contact email when available -->
<!-- security@tellstone.dev -->

If you prefer email and have a PGP key, encrypt your report. Otherwise, include as much detail as possible in plain text.

## What to Include

- **Description** — What is the vulnerability? How does it work?
- **Affected versions** — Which versions of Tellstone are impacted?
- **Attack vector** — How can this be exploited? (network, local, configuration)
- **Severity** — Your assessment of the impact (Critical / High / Medium / Low)
- **Reproduction steps** — Minimal steps to demonstrate the issue
- **Suggested fix** — If you have one

## Response Process

1. **Acknowledgment** — We aim to acknowledge reports within **48 hours**.
2. **Assessment** — We will evaluate the report, determine severity, and identify affected versions.
3. **Fix** — We will develop and test a fix. For critical issues, this is prioritized.
4. **Disclosure** — We will coordinate disclosure with the reporter. A CVE may be requested if applicable.
5. **Release** — The fix is released and a security advisory is published.

## Scope

In scope:
- Tellstone server binary (`cmd/tellstone`)
- All packages under `internal/`
- Binary protocol (`internal/network`)
- Storage engine, persistence, crypto, metrics, tracing

Out of scope:
- Third-party dependencies (report upstream)
- Issues requiring physical access to the host
- Social engineering attacks

## Supported Versions

| Version | Supported          |
|---------|--------------------|
| main    | :white_check_mark: |

## Severity Definitions

| Severity | Description |
|----------|-------------|
| **Critical** | Remote code execution, authentication bypass, data exfiltration without auth |
| **High** | Denial of service, privilege escalation, significant data leak with auth |
| **Medium** | Limited information disclosure, minor logic flaw requiring specific conditions |
| **Low** | Theoretical issues, requires unusual configuration or user interaction |

---

We appreciate your help in keeping Tellstone and its users safe.
