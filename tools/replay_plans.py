from datetime import datetime
from typing import Dict


REPLAY_RULES = {
    "order_created": {
        "bronze": {
            "job": "glue-streaming-order-created",
            "replayable": True
        },
        "silver": {
            "job": "bronze-to-silver-order-created",
            "replayable": True
        },
        "gold": {
            "job": "silver-to-gold-daily-order-metrics",
            "replayable": True
        }
    }
}


def build_replay_plan(
    dataset: str,
    start_date: str,
    end_date: str
) -> Dict:
    """
    Build a dry-run replay plan for a dataset and date range.
    """
    # Validate dates
    try:
        start = datetime.fromisoformat(start_date)
        end = datetime.fromisoformat(end_date)
    except ValueError:
        raise ValueError("Dates must be in ISO format (YYYY-MM-DD)")

    if start > end:
        raise ValueError("start_date must be <= end_date")

    # Map dataset to flow
    if dataset.startswith(("bronze_", "silver_", "gold_")):
        flow_key = "order_created"
    else:
        raise ValueError(f"No replay rules registered for '{dataset}'")

    rules = REPLAY_RULES[flow_key]

    plan = {
        "dataset": dataset,
        "flow": flow_key,
        "window": {
            "start_date": start_date,
            "end_date": end_date
        },
        "replay_path": [],
        "impacted_layers": [],
        "safety_checks": [
            "bronze_is_immutable",
            "silver_upsert_idempotent",
            "gold_rebuildable"
        ]
    }

    # Always replay from bronze forward
    for layer in ["bronze", "silver", "gold"]:
        rule = rules[layer]
        plan["replay_path"].append({
            "layer": layer,
            "job": rule["job"],
            "replayable": rule["replayable"]
        })
        plan["impacted_layers"].append(layer)

    return plan