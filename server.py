from fastapi import FastAPI
from typing import Optional, List

from mcp.server.fastapi import MCPFastAPI
from mcp.types import ToolResponse


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
# Mock data source (will be replaced by AWS calls)
# -------------------------------------------------

DATASETS = [
    {"name": "bronze_order_created", "layer": "bronze"},
    {"name": "silver_order_created", "layer": "silver"},
    {"name": "gold_daily_order_metrics", "layer": "gold"},
]


# -------------------------------------------------
# MCP Tools (Read-only, safe)
# -------------------------------------------------

@mcp.tool()
def list_datasets(layer: Optional[str] = None) -> ToolResponse:
    """
    List available datasets in the data platform.

    Parameters:
    - layer: Optional filter (bronze, silver, gold)

    This tool is read-only and safe.
    """
    if layer:
        filtered = [
            d["name"] for d in DATASETS if d["layer"] == layer.lower()
        ]
    else:
        filtered = [d["name"] for d in DATASETS]

    return ToolResponse(
        content={
            "datasets": filtered,
            "count": len(filtered),
        }
    )


# -------------------------------------------------
# Health check (non-MCP, for ops)
# -------------------------------------------------

@app.get("/health")
def health():
    return {"status": "ok"}