# Contributing

This is a personal portfolio project, but issues and pull requests are welcome.

- **Before a pull request:** run `go vet ./...`, `go test ./...`, `(cd worker && uv run pytest -q)` and, for web changes, `(cd web && npm run lint && npm run typecheck && npm test)`. Database and real-sandbox tests are described in the [usage reference](docs/usage.zh-CN.md). CI runs all of them, including root tests in real sandboxes.
- **API changes** start in [`api/openapi.yaml`](api/openapi.yaml). Regenerate the web types with `npm run gen:api`; CI checks that they match.
- **Worker protocol changes** need matching Go and Python changes and updated fixtures in [`protocol/`](protocol/README.md).
- **Database changes** are new numbered migrations; never edit an applied one.
- Keep secrets out of commits, logs and test fixtures. Keys are read only from environment variables.
