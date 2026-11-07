---
name: untangle-codebase
description: Untangle a whole codebase in one pass, so agents working in it read less per change, break less without noticing, and debug faster.
disable-model-invocation: true
argument-hint: "[optional area or concern to focus on]"
---
Start a thorough refactor of this codebase and complete it to the end. Treat this as a goal.

Ask whether to add **Comments** — “Remove comments that only restate the code.” and/or **Tests** — “Remove tests that wouldn't catch a real bug, and fix the ones that fail without one.” Do only selected additions.

Don't change anything during inventory. List every hidden link (duplicated logic/value, implicit global/file coupling, features split by layer/folder, implicit call order/convention), needless link (callers depend on internals, stable code imports volatile code, workflow-specific arguments in general functions, shallow modules), surplus (pass-through wrappers, speculative extension/config, dead code), and special case (one-value if, caller-specific parameter, swallowed exception, retry/sleep, disabled test).

Redesign from first principles, record fixes above the problems they solve, then fix the list. Keep necessary special cases; remove ones the redesign eliminates. If Comments selected: remove comments restating implementation; keep external constraints; remove workaround comments when the workaround goes. If Tests selected: do this last; remove implementation-mirroring/ineffective tests and rewrite tests coupled to internals as behavior tests.

Repeat until there is nothing worth fixing. Check that typical changes land in one place, imports are reasonable and dependency direction follows volatility, and intended behavior is preserved. Have subagents search for special cases, hidden links, needless links and surplus. Use Git history and dependency graphs where useful.
