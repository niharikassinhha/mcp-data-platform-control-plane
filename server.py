from fastapi import FastAPI
from typing import Optional, List
from aws.glue import GlueCatalogClient
from aws.config import GLUE_DATABASES_BY_LAYER
from mcp.server.fastapi import MCPFastAPI
from mcp.types import ToolResponse
from aws.resolver import resolve_dataset
from aws.athena import AthenaClient
from tools.lineage import DATA_FLOWS
from tools.replay_plans import build_replay_plan
# -------------------------------------------------
# FastAPI + MCP bootstrap
# -------------------------------------------------

app = FastAPI(
    title="Data Platform Control Plane (MCP)",
    description="MCP server exposing governed controls over a data platform",
    version="0.1.0",
)

mcp = MCPFastAPI(
    app=app,
    name="data-platform-control-plane",
    description=(
        "A control plane MCP server that allows AI agents to safely "
        "inspect and operate a data platform."
    ),
)


# -------------------------------------------------
# MCP Tools (Read-only, safe)
# -------------------------------------------------

@mcp.tool()
def list_datasets(layer: Optional[str] = None) -> ToolResponse:
    """
    List available datasets in the data platform using AWS Glue Catalog.

    Parameters:
    - layer: Optional filter (bronze, silver, gold)

    This tool is read-only and safe.
    """
    glue = GlueCatalogClient()

    datasets = []

    if layer:
        layer = layer.lower()
        if layer not in GLUE_DATABASES_BY_LAYER:
            return ToolResponse(
                content={"error": f"Unknown layer '{layer}'"}
            )

        db = GLUE_DATABASES_BY_LAYER[layer]
        tables = glue.list_tables(db)

        datasets = [t["name"] for t in tables]

    else:
        for layer_name, db in GLUE_DATABASES_BY_LAYER.items():
            tables = glue.list_tables(db)
            for t in tables:
                datasets.append({
                    "dataset": t["name"],
                    "layer": layer_name,
                })

    return ToolResponse(
        content={
            "datasets": datasets,
            "count": len(datasets),
        }
    )

@mcp.tool()
def get_dataset_schema(dataset: str) -> ToolResponse:
    """
    Return schema and partitioning information for a dataset.

    Parameters:
    - dataset: Dataset name (e.g. silver_order_created or silver.silver_order_created)

    This tool is read-only and uses metadata only.
    """
    glue = GlueCatalogClient()

    try:
        database, table = resolve_dataset(dataset)
    except ValueError as e:
        return ToolResponse(content={"error": str(e)})

    try:
        table_info = glue.get_table(database, table)
    except Exception as e:
        return ToolResponse(
            content={"error": f"Failed to fetch schema: {str(e)}"}
        )

    return ToolResponse(
        content={
            "dataset": f"{database}.{table}",
            "columns": table_info["columns"],
            "partitions": table_info["partitions"],
            "table_type": table_info["table_type"],
            "table_properties": table_info["parameters"],
        }
    )

@mcp.tool()
def get_pipeline_status(dataset: str, limit: int = 5) -> ToolResponse:
    """
    Return recent pipeline execution status and dataset health.

    Parameters:
    - dataset: dataset name (e.g. silver_order_created)
    - limit: number of recent job runs to return

    Read-only. Safe.
    """
    athena = AthenaClient()

    try:
        job_runs = athena.run_query(
            query=f"""
                SELECT
                    job_name,
                    job_type,
                    status,
                    start_time,
                    end_time,
                    records_processed,
                    error_message
                FROM monitoring.job_runs
                WHERE dataset = '{dataset}'
                ORDER BY start_time DESC
                LIMIT {limit}
            """,
            database="monitoring",
        )

        dataset_status = athena.run_query(
            query=f"""
                SELECT
                    dataset,
                    layer,
                    last_success_time,
                    last_record_count,
                    freshness_minutes
                FROM monitoring.dataset_status
                WHERE dataset = '{dataset}'
            """,
            database="monitoring",
        )

    except Exception as e:
        return ToolResponse(
            content={"error": f"Failed to fetch pipeline status: {str(e)}"}
        )

    return ToolResponse(
        content={
            "dataset": dataset,
            "recent_runs": job_runs,
            "current_status": dataset_status[0] if dataset_status else None,
        }
    )

@mcp.tool()
def explain_data_flow(dataset: str) -> ToolResponse:
    """
    Explain how data flows through the platform for a given dataset.

    Parameters:
    - dataset: dataset name (e.g. silver_order_created, gold_daily_order_metrics)

    This tool is read-only and reasoning-only.
    """
    # Normalize dataset name to flow key
    if dataset.startswith("bronze_") or dataset.startswith("silver_") or dataset.startswith("gold_"):
        flow_key = "order_created"
    else:
        return ToolResponse(
            content={"error": f"No data flow registered for dataset '{dataset}'"}
        )

    flow = DATA_FLOWS.get(flow_key)

    if not flow:
        return ToolResponse(
            content={"error": f"No data flow found for '{dataset}'"}
        )

    return ToolResponse(
        content={
            "dataset": dataset,
            "flow": flow
        }
    )
@mcp.tool()
def propose_replay_plan(
    dataset: str,
    start_date: str,
    end_date: str
) -> ToolResponse:
    """
    Propose a dry-run replay plan for correcting data.

    Parameters:
    - dataset: dataset name (e.g. silver_order_created)
    - start_date: YYYY-MM-DD
    - end_date: YYYY-MM-DD

    This tool does NOT execute anything.
    """
    try:
        plan = build_replay_plan(
            dataset=dataset,
            start_date=start_date,
            end_date=end_date
        )
    except ValueError as e:
        return ToolResponse(content={"error": str(e)})

    return ToolResponse(
        content={
            "replay_plan": plan,
            "requires_approval": True,
            "execution_disabled": True
        }
    )
# -------------------------------------------------
# Health check (non-MCP, for ops)
# -------------------------------------------------

@app.get("/health")
def health():
    return {"status": "ok"}