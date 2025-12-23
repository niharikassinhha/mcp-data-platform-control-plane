from fastapi import FastAPI
from typing import Optional, List
from aws.glue import GlueCatalogClient
from aws.config import GLUE_DATABASES_BY_LAYER
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

# -------------------------------------------------
# Health check (non-MCP, for ops)
# -------------------------------------------------

@app.get("/health")
def health():
    return {"status": "ok"}