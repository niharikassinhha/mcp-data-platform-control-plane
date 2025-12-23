import boto3
from typing import List, Dict


class GlueCatalogClient:
    """
    Read-only client for AWS Glue Data Catalog.
    """

    def __init__(self, region: str = "us-east-1"):
        self.client = boto3.client("glue", region_name=region)

    def list_databases(self) -> List[str]:
        paginator = self.client.get_paginator("get_databases")
        databases = []

        for page in paginator.paginate():
            for db in page["DatabaseList"]:
                databases.append(db["Name"])

        return databases

    def list_tables(self, database: str) -> List[Dict]:
        paginator = self.client.get_paginator("get_tables")
        tables = []

        for page in paginator.paginate(DatabaseName=database):
            for table in page["TableList"]:
                tables.append({
                    "database": database,
                    "name": table["Name"],
                    "table_type": table.get("TableType"),
                })

        return tables

    def get_table(self, database: str, table_name: str) -> Dict:
        response = self.client.get_table(
            DatabaseName=database,
            Name=table_name
        )
        table = response["Table"]

        columns = table["StorageDescriptor"]["Columns"]
        partition_keys = table.get("PartitionKeys", [])

        return {
            "database": database,
            "table": table_name,
            "columns": [
                {"name": c["Name"], "type": c["Type"]}
                for c in columns
            ],
            "partitions": [
                {"name": p["Name"], "type": p["Type"]}
                for p in partition_keys
            ],
            "table_type": table.get("TableType"),
            "parameters": table.get("Parameters", {}),
        }