from aws.config import GLUE_DATABASES_BY_LAYER


def resolve_dataset(dataset: str):
    """
    Resolve dataset name to (database, table).
    Supports:
      - silver_order_created
      - silver.silver_order_created
    """
    if "." in dataset:
        layer, table = dataset.split(".", 1)
        if layer not in GLUE_DATABASES_BY_LAYER:
            raise ValueError(f"Unknown layer '{layer}'")
        return GLUE_DATABASES_BY_LAYER[layer], table

    # infer layer by database scan order
    for layer, db in GLUE_DATABASES_BY_LAYER.items():
        return db, dataset

    raise ValueError(f"Unable to resolve dataset '{dataset}'")