---
name: state-machine-audit
description: Find implicit state machines in an application and, when requested, align state definitions, transitions, and invariants across code, storage, and documentation.
argument-hint: "[optional subsystem or scope]"
---

Map application state machines from their actual state, transitions, and owners.
Use this skill for requests to find, explain, audit, or reconcile implicit
state machines and status values in a codebase.

## Workflow

1. Read repository guidance and identify the application boundaries. Trace
   runtime code, schemas and migrations, API contracts, configuration, and
   relevant docs. Search for status/state constants, enum or check constraints,
   state-dependent branches, conditional updates, timestamps and nullable-field
   combinations, locks/leases, and lifecycle flags.
2. Group findings by entity or process. For each machine, establish:
   - its states and the data that represents them;
   - events or guards that cause transitions;
   - the code or subsystem that owns each transition;
   - side effects and concurrency rules that are part of the transition.
3. Distinguish active transitions from states that are only permitted by a
   schema, named in docs, or planned for a component that is absent. Do not
   present a schema enum as proof that its transitions are implemented.
4. Report evidence with clickable local file references when available. Give a
   concise transition table or diagram for complex machines. Call out missing,
   conflicting, or duplicated definitions and state the limits of the audit.

## When the user asks to fix or align states

After mapping the relevant machines, make the smallest coherent changes that
remove identified contradictions or drift. Keep behavior stable unless the
user asks to change it. Prefer one authoritative definition per layer, and
align API handling, persistence constraints, and documentation with it. Use a
new migration for an already-applied schema change; include a valid rollback
when the repository's migration convention supports one.

Do not invent transitions or add a scheduler, worker, relay, or resolver just
to make every schema state reachable. If those components or their behavior
are unspecified, document which transitions are absent and what owns them
once implemented. Keep unrelated refactors out of scope.

## Scope and evidence

Treat combinations of fields, idempotency records, leases, cancellation
requests, transaction branches, and process shutdown flags as possible
implicit state, but include them only when they encode meaningful lifecycle or
transition behavior. Exclude ordinary pagination and one-off validation
branches unless they form a persistent or multi-step lifecycle.

Do not claim the audit is exhaustive beyond the repository and scope inspected.
When a subsystem has no transition owner in the codebase, identify it as
schema-only, documented-only, or planned rather than guessing its runtime
behavior.
