import boto3
import time
from typing import List, Dict


class AthenaClient:
    def __init__(self, region: str = "us-east-1", workgroup: str = "primary"):
        self.client = boto3.client("athena", region_name=region)
        self.workgroup = workgroup

    def run_query(self, query: str, database: str) -> List[Dict]:
        response = self.client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": database},
            WorkGroup=self.workgroup,
        )

        execution_id = response["QueryExecutionId"]

        # Poll until finished
        while True:
            status = self.client.get_query_execution(
                QueryExecutionId=execution_id
            )["QueryExecution"]["Status"]["State"]

            if status in ("SUCCEEDED", "FAILED", "CANCELLED"):
                break
            time.sleep(0.5)

        if status != "SUCCEEDED":
            raise RuntimeError(f"Athena query failed with status {status}")

        result = self.client.get_query_results(
            QueryExecutionId=execution_id
        )

        rows = result["ResultSet"]["Rows"]
        headers = [c["VarCharValue"] for c in rows[0]["Data"]]

        data = []
        for row in rows[1:]:
            values = [d.get("VarCharValue") for d in row["Data"]]
            data.append(dict(zip(headers, values)))

        return data