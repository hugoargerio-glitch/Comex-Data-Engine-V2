---
name: post-incident-learn
description: Structured post-incident learning workflow (Self-Healing Governance) that converts production failures into stricter ODCS v3.2.0 contracts, Elementary/dbt tests, and decision trees in ./knowledge/runbooks/.
version: 1.0.0
owner: dataops-team
invoked_by: ["dataops-incident-triage", "data-quality-auditor"]
audit_trail: git-pr
---

# Skill: Post-Incident Learning & Self-Healing Loop (`post-incident-learn.md`)

This procedure transforms resolved operational incidents into executable institutional memory auditable via Git Pull Request. No `P0`, `P1`, or `P2` incident is considered closed until the 5 steps below are completed by the `dataops` subagent.

- **MANDATE**: Convert every identified root cause into an automated deterministic check (`datacontract test`, dbt SQL test, or `elementary-observability` monitor) bound to its corresponding `./contracts/*.odcs.yaml` contract.
- **!ANTI_PATTERN**: Never close a post-mortem with vague textual advice ("pay closer attention during code review") without adding an executable technical guardrail to the repository.

---

## Step 1 — Extract the Deterministic Failure Signature
From the diagnostic completed under `./knowledge/runbooks/INCIDENT_TEMPLATE.md`, document with surgical precision:
1. **Affected Asset & Contract**: ODCS v3.2.0 contract URN and `datahub` dataset URN.
2. **Failure Vector**:
   - *Schema Drift*: which column changed physical type or which unannounced enum value appeared in Bronze.
   - *Fan-Out / Grain Violation*: which `JOIN` or deduplication window caused row multiplication.
   - *Financial Drift*: which edge case (`NULL`, partial refund, timezone shift) violated the reconciliation identity.
3. **Detection Gap (Why the Gate Missed It)**: Why the existing suite of `datacontract test`, `dbt test`, and `elementary` allowed the issue to reach production (or what was missing to contain it in `_quarantine`).

## Step 2 — Harden the Data Contract (`./contracts/*.odcs.yaml`)
Edit the corresponding ODCS v3.2.0 contract to immunize the pipeline against recurrence:
1. **New Assertion in `quality` or `context.constraints`**:
   - Add a deterministic SQL check under `schema.quality` or a negative constraint under `context.constraints` that catches the exact failure condition.
2. **Paired Guidance in `context.instructions`**:
   - Add the paired `MANDATE:` and `!ANTI_PATTERN:` directive directly into the contract's `context.instructions` block to guide future runs of `planner` and `worker`.
3. **Immediate Contract Validation**:
   - Run `make contract-lint` and `make contract-test` (`datacontract test --server local_duckdb`) to prove the new guardrail is syntactically valid and executable.

## Step 3 — Update Institutional Memory (`./knowledge/`)
1. **In `./knowledge/domain/semantic_rules.md`**:
   - If the incident exposed a business edge case (e.g., new payment status or timezone rule), document the updated formula and its paired `MANDATE:` / `!ANTI_PATTERN:` rule.
2. **In `./knowledge/runbooks/INCIDENT_TEMPLATE.md` (or specific runbook)**:
   - Add the symptom $\rightarrow$ diagnostic query $\rightarrow$ containment action pattern to the decision tree so future occurrences are triaged deterministically in seconds.

## Step 4 — Cross-Verification by `auditor` Gate
1. Submit the changes across `./contracts/`, `./knowledge/`, and `./dbt_project/` or `./pipelines/` to the `auditor` subagent.
2. Confirm that `datacontract lint`, `bruin validate`, and `dbt compile` return `PASS`.

## Step 5 — Auditable Packaging via Git Pull Request
Structure the final commit / PR description containing:
- **Incident Reference**: `[INC-YYYY-MM-DD-XXX]`
- **Root Cause Summary**: Concise technical summary of the root cause and blast radius observed in `datahub`.
- **Immunization Diff Summary**:
  - Updated files in `./contracts/*.odcs.yaml`
  - Updated files in `./knowledge/domain/` and `./knowledge/runbooks/`
  - Executed verification commands with `PASS` evidence.
