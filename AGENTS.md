# Agent instructions

Before performing repository tasks, inspect the applicable documents in
`.dev/`. They contain local development and testing instructions that are not
part of the tracked project documentation.

Keep `.dev/` private and local: do not copy its environment-specific values,
cluster details, credentials, endpoints, or other operational information into
tracked files, command output committed to the repository, or user-facing
documentation. The directory is intentionally gitignored.

## Testing

Keep each test focused on one behavior or outcome. A test may cover multiple
assertions when they all describe that same behavior. Keep multiple behaviors
in one test only when their preparation steps, such as injecting test data,
are the same and splitting the test would duplicate that setup.

If a test fails because the restricted environment prevents a test listener
from binding, rerun the affected test outside the sandbox before treating the
failure as a code or test regression.
