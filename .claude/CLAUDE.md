@AGENTS.md

# Claude Code Runtime Shim
- Inherit all canonical rules, Three-Tier Boundaries, and validation commands from `@AGENTS.md`.
- Delegate specialized tasks strictly via native Markdown subagents in `.claude/agents/{planner,worker,auditor,dataops}.md`.
- Load MCP servers strictly per subagent frontmatter from `.agents/mcp_config.json` to preserve main context token budget.
- Execute all pipeline builds (`worker`) and contract audits (`auditor`) inside isolated Git worktrees (`isolation: worktree`).
