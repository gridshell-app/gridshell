# Security Policy

## Reporting a vulnerability

Please don't open a public GitHub issue for a security vulnerability. Instead:

- **Preferred**: use [GitHub's private vulnerability reporting](https://github.com/gridshell-app/gridshell/security/advisories/new) for this repository. It creates a private draft advisory only you and the maintainer can see, so nothing is disclosed before a fix is ready.
- **Fallback**: email gridshell.app@gmail.com.

Please include enough detail to reproduce the issue (which component - the Sheets add-on or the `gridshell` server/library, affected version, steps or a proof of concept).

## What to expect

This is a solo-maintained project, so there's no guaranteed SLA, but reports are acknowledged as promptly as possible, typically within a few days. Please don't publicly disclose a vulnerability until a fix has shipped and there's been a chance to coordinate timing with you.

## Scope

Both components are in scope for reports, even though only one is in this repository: the self-hosted server + Python client (this repository) and the Google Sheets add-on itself (closed source, distributed only through the Google Workspace Marketplace - not in this repository, but still very much a live attack surface worth reporting issues against). GridShell's existing security model and known guardrails (URL Guard, the auth token, deployment hardening) are documented in [Server: Security & Deployment](https://gridshell.app/library/server/#security-deployment) and [App Settings & Limitations](https://gridshell.app/app/settings-and-limitations/) - a real bypass of one of those is exactly the kind of thing worth reporting here.
