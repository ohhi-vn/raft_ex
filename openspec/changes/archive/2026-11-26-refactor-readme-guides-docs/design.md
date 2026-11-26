# Design

## Context

Documentation lives in three places: `README.md` (status + quick start), `guides/` (task-oriented), and `docs/` (reference). They have drifted from implemented features and from each other. OpenSpec specs under `openspec/specs/` are the behavioural source of truth.

## Goals / Non-Goals

**Goals:**
- Make README status accurate and consistent with specs.
- Bring stub guides to a usable minimum.
- Fix internal links and references.

**Non-Goals:**
- Rewriting API docs (handled by `mix docs`).
- Changing code or specs.

## Decisions

### D1: README status derived from specs
Use `openspec/specs/` capabilities as the source of truth for the status list.

### D2: Stub guides expanded in place
Keep existing file names/paths to avoid breaking links; fill content.

## Risks / Trade-offs

- [Docs drift recurs] → Add a checklist item in release checklist to verify README status.

## Migration Plan

1. Audit links and status.
2. Edit docs.
3. Run `mix docs --warnings-as-errors`.
