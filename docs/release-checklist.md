# Release checklist

1. Run `mix precommit` (compile warnings-as-errors, format, credo, tests).
2. Run `mix release.check` (version sync across mix.exs, CHANGELOG, docs source_ref).
3. Update `docs/feature-status` table and README feature status matrix.
4. Update CHANGELOG.md with the new version heading and notes.
5. Bump `@version` in `mix.exs`.
6. Tag the release `vX.Y.Z` and push.
7. Publish to Hex (`mix hex.publish`).
8. Generate docs (`mix docs`) and verify no warnings.