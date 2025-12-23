# Data Platform Control Plane (MCP)

This project implements a **Model Context Protocol (MCP) server** that acts as a
**governed control plane** for a modern data platform.

Instead of allowing unrestricted access to data or infrastructure, this control
plane exposes **intent-based, read-only and approval-gated tools** that allow
AI agents (and humans) to safely inspect, reason about, and plan operations on
the data platform.

---

## What This Is (and Is Not)

### This IS
- A control plane for data platforms
- Metadata- and reasoning-driven
- Safe for AI agents to interact with
- Designed for enterprise-scale operations
- Explicitly governed and auditable

### This is NOT
- “Chat with your data”
- Free-form SQL execution
- AI-triggered pipelines without approval
- An orchestration engine

---

## Architecture Overview
AI Agent / LLM
↓  (MCP)
Data Platform Control Plane Server
↓ (read-only)
•	AWS Glue Catalog
•	Iceberg metadata
•	Monitoring tables (Athena)
↓ (planning only)
•	Replay planning logic

The control plane **never directly mutates data or infrastructure**.

---

## Core Capabilities

### Dataset Discovery
- Enumerate datasets by layer (bronze, silver, gold)
- Backed by AWS Glue Catalog

### Schema Inspection
- Retrieve schema, partitions, and table properties
- Metadata-only (no data scans)

### Pipeline Health
- Inspect recent job runs
- Check freshness and failure states
- Backed by operational metadata tables

### Lineage & Flow Explanation
- Explain how data moves from source → Bronze → Silver → Gold
- Includes SLAs and replay strategy
- Deterministic and versioned (no inference)

### Replay Planning (Dry-Run)
- Propose replay plans for a time window
- Show impacted layers and jobs
- Explicitly approval-gated
- Execution intentionally disabled

---

## MCP Tools Exposed

| Tool | Purpose | Mutates State |
|----|--------|---------------|
| list_datasets | Discover datasets | ❌ No |
| get_dataset_schema | Inspect schema & partitions | ❌ No |
| get_pipeline_status | Check job health & freshness | ❌ No |
| explain_data_flow | Explain lineage & SLAs | ❌ No |
| propose_replay_plan | Plan a replay (dry-run) | ❌ No |

---

## Demo Scenarios (Interview-Ready)

### Scenario 1 — “Why is yesterday’s revenue wrong?”

**Agent flow:**
1. `get_pipeline_status(dataset="gold_daily_order_metrics")`
2. Detects delayed Silver processing
3. `explain_data_flow(dataset="gold_daily_order_metrics")`
4. Identifies Bronze → Silver → Gold dependency
5. `propose_replay_plan(start_date="2025-01-14", end_date="2025-01-15")`

**Outcome:**
- Root cause identified
- Replay plan proposed
- No automatic execution

---

### Scenario 2 — “Can I change this schema safely?”

**Agent flow:**
1. `get_dataset_schema(dataset="silver_order_created")`
2. Reviews partitions and column types
3. `explain_data_flow(dataset="silver_order_created")`
4. Identifies downstream Gold dependency

**Outcome:**
- Human makes informed schema decision
- No guesswork, no hidden coupling

---

### Scenario 3 — “What datasets exist and where?”

**Agent flow:**
1. `list_datasets(layer="gold")`

**Outcome:**
- Authoritative discovery from Glue Catalog
- No tribal knowledge required

---

## Safety & Guardrails

This MCP server enforces safety through:

- Read-only AWS permissions
- Explicit dataset-to-layer mapping
- Static lineage registry
- No SQL execution
- No pipeline triggers
- Approval-required replay planning
- Execution intentionally disabled

This makes the control plane safe for AI usage in enterprise environments.

---

## Why MCP for a Data Platform?

Traditional REST or CLI interfaces expose **capabilities**.

MCP exposes **intent**.

This allows AI agents to:
- Ask *why* something is broken
- Reason about blast radius
- Propose corrective actions
- Stay within governance boundaries

---

## Status

This project is intentionally scoped to:
- Control-plane reasoning
- Metadata inspection
- Operational planning

Execution and orchestration are out of scope by design.

---

## Who This Is For

- Lead / Principal Data Engineers
- Platform teams
- Organizations exploring AI-assisted operations
- Interview demonstrations of system design + judgment