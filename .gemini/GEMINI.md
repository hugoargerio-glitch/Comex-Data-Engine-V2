@AGENTS.md

# Gemini CLI & Google Antigravity Runtime Shim
- Inherit all canonical rules, Three-Tier Boundaries, and validation commands from `@AGENTS.md`.
- Always use native `define_subagent` and `invoke_subagent` tools to spawn `.claude/agents/{planner,worker,auditor,dataops}.md` in isolated subagent contexts; never roleplay subagents in the main thread.
- Enforce the Flash-First policy: `planner`, `worker` (by default), `auditor`, and `dataops` run on `Model: 'flash'` (`thinkingEffort: low`) to preserve the weekly Pro plan quota (168h window).
- Elevate `worker` to `Model: 'pro'` (Argon 4 Low) strictly upon the 2-Failure Fallback Rule or for critical algorithmic logic.
- Upon receiving human `APPROVED` after the planner's specification gate, automatically invoke `worker` (in Flash) and then `auditor` in sequence.
- Never reference unversioned external notebooks; persist all architectural decisions and RCA learnings directly in Git.
