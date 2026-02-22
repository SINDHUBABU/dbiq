import boto3
import pandas as pd
import time
import re

# ======================================================
# CONFIG
# ======================================================

REGION = "us-east-1"
S3_OUTPUT = "s3://dbiq-agent-sindhu-2025/athena-results/"

athena = boto3.client("athena", region_name=REGION)

# ======================================================
# Get List of Databases
# ======================================================
def get_databases():
    response = athena.list_databases(CatalogName="AwsDataCatalog")
    return [db["Name"] for db in response["DatabaseList"]]

# ======================================================
# Get Tables in a Database
# ======================================================
def get_tables(database):
    response = athena.list_table_metadata(
        CatalogName="AwsDataCatalog",
        DatabaseName=database
    )
    return [table["Name"] for table in response["TableMetadataList"]]

# ======================================================
# Extract Tables from Query
# ======================================================
def extract_tables_from_query(query):
    tables = re.findall(r'FROM\s+([a-zA-Z0-9_.]+)', query, re.IGNORECASE)
    joins = re.findall(r'JOIN\s+([a-zA-Z0-9_.]+)', query, re.IGNORECASE)
    all_tables = list(set(tables + joins))
    return ", ".join(all_tables)

# ======================================================
# Get Last 100 Queries
# ======================================================
def get_recent_queries(limit=100):

    response = athena.list_query_executions(MaxResults=50)
    query_ids = response["QueryExecutionIds"]

    if "NextToken" in response:
        next_response = athena.list_query_executions(
            MaxResults=50,
            NextToken=response["NextToken"]
        )
        query_ids += next_response["QueryExecutionIds"]

    query_ids = query_ids[:limit]

    results = []

    for qid in query_ids:
        details = athena.get_query_execution(QueryExecutionId=qid)
        stats = details["QueryExecution"]["Statistics"]
        status_info = details["QueryExecution"]["Status"]
        context = details["QueryExecution"].get("QueryExecutionContext", {})

        query_text = details["QueryExecution"]["Query"]
        database_used = context.get("Database", "Unknown")

        data_scanned_bytes = stats.get("DataScannedInBytes", 0)
        data_scanned_mb = data_scanned_bytes / (1024 * 1024)
        cost_usd = (data_scanned_bytes / (1024 ** 4)) * 5

        results.append({
            "QueryID": qid,
            "Database": database_used,
            "Tables_Used": extract_tables_from_query(query_text),
            "Status": status_info["State"],
            "ExecutionTime_ms": stats.get("EngineExecutionTimeInMillis", 0),
            "DataScanned_MB": round(data_scanned_mb, 2),
            "Cost_USD": round(cost_usd, 6),
            "Error_Message": status_info.get("StateChangeReason", "")
        })

    return pd.DataFrame(results)

# ======================================================
# SQL Optimization Suggestions
# ======================================================
def suggest_sql_rewrite(query):

    optimized_query = query
    suggestions = []
    upper_query = query.upper()

    if "SELECT *" in upper_query:
        suggestions.append("Avoid SELECT *. Specify required columns.")
        optimized_query = re.sub(
            r"SELECT \*",
            "SELECT column1, column2",
            optimized_query,
            flags=re.IGNORECASE
        )

    if "WHERE" not in upper_query:
        suggestions.append("No WHERE clause detected. Adding filter.")
        optimized_query += "\nWHERE some_column > 0"

    if "CROSS JOIN" in upper_query:
        suggestions.append("CROSS JOIN detected. Converting to INNER JOIN.")
        optimized_query = re.sub(
            r"CROSS JOIN",
            "JOIN",
            optimized_query,
            flags=re.IGNORECASE
        )
        optimized_query += "\nON a.id = b.id"

    if "LIMIT" not in upper_query:
        suggestions.append("LIMIT clause added.")
        optimized_query += "\nLIMIT 100"

    if not suggestions:
        suggestions.append("Query looks optimized.")

    return suggestions, optimized_query

# ======================================================
# Run Query + Auto Kill + Duplicate Fix
# ======================================================
def run_athena_query(query,
                     database,
                     output_location=S3_OUTPUT,
                     kill_after_seconds=25):

    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": output_location},
    )

    query_execution_id = response["QueryExecutionId"]
    start_time = time.time()

    while True:
        status = athena.get_query_execution(QueryExecutionId=query_execution_id)
        state = status["QueryExecution"]["Status"]["State"]

        elapsed = time.time() - start_time

        if elapsed > kill_after_seconds and state == "RUNNING":
            athena.stop_query_execution(QueryExecutionId=query_execution_id)
            return None, "KILLED", "Query automatically stopped due to long execution."

        if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            break

        time.sleep(1)

    if state == "SUCCEEDED":
        results = athena.get_query_results(QueryExecutionId=query_execution_id)

        columns = [
            col["Label"]
            for col in results["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]
        ]

        rows = results["ResultSet"]["Rows"][1:]

        data = []
        for row in rows:
            data.append([col.get("VarCharValue", "") for col in row["Data"]])

        df = pd.DataFrame(data, columns=columns)

        # 🔥 SAFE DUPLICATE COLUMN HANDLING (Modern Pandas Compatible)
        new_columns = []
        seen = {}

        for col in df.columns:
            if col in seen:
                seen[col] += 1
                new_columns.append(f"{col}_{seen[col]}")
            else:
                seen[col] = 0
                new_columns.append(col)

        df.columns = new_columns

        return df, state, None

    error_message = status["QueryExecution"]["Status"].get("StateChangeReason", "Unknown error")
    return None, state, error_message
# ======================================================
# Get Single Query Detailed Info
# ======================================================
def get_query_detailed_info(query_execution_id):

    details = athena.get_query_execution(QueryExecutionId=query_execution_id)

    stats = details["QueryExecution"]["Statistics"]
    status_info = details["QueryExecution"]["Status"]
    context = details["QueryExecution"].get("QueryExecutionContext", {})
    query_text = details["QueryExecution"]["Query"]

    data_scanned_bytes = stats.get("DataScannedInBytes", 0)
    data_scanned_mb = data_scanned_bytes / (1024 * 1024)
    cost_usd = (data_scanned_bytes / (1024 ** 4)) * 5

    execution_time = stats.get("EngineExecutionTimeInMillis", 0)

    # Detect Query Type
    query_type = query_text.strip().split(" ")[0].upper()

    return {
        "Query_ID": query_execution_id,
        "Query_Type": query_type,
        "Execution_Time_ms": execution_time,
        "Data_Retrieved_MB": round(data_scanned_mb, 2),
        "Status": status_info["State"],
        "Cost_USD": round(cost_usd, 6)
    }

