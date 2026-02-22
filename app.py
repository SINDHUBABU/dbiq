from monitor import get_recent_queries
from monitor import get_recent_queries, get_query_detailed_info
import streamlit as st
import boto3
import requests
import pandas as pd
import time
import re

# ==============================
# CONFIGURATION
# ==============================

REGION = "us-east-1"

COGNITO_DOMAIN = "https://us-east-170qtk9rmx.auth.us-east-1.amazoncognito.com"
CLIENT_ID = "6fp86c763nr21onqnci9upv8cr"
REDIRECT_URI = "http://localhost:8501"

S3_OUTPUT = "s3://dbiq-agent-sindhu-2025/athena-results/"

# ==============================
# AWS CLIENTS
# ==============================

athena = boto3.client("athena", region_name=REGION)
dynamodb = boto3.resource("dynamodb", region_name=REGION)
table = dynamodb.Table("DBIQ_Users")

# ==============================
# AUTH FUNCTIONS
# ==============================

def get_login_url():
    return (
        f"{COGNITO_DOMAIN}/oauth2/authorize"
        f"?client_id={CLIENT_ID}"
        f"&response_type=code"
        f"&scope=openid+email"
        f"&redirect_uri={REDIRECT_URI}"
    )

def get_logout_url():
    return (
        f"{COGNITO_DOMAIN}/logout"
        f"?client_id={CLIENT_ID}"
        f"&logout_uri={REDIRECT_URI}"
    )

def exchange_code_for_token(code):
    token_url = f"{COGNITO_DOMAIN}/oauth2/token"
    data = {
        "grant_type": "authorization_code",
        "client_id": CLIENT_ID,
        "code": code,
        "redirect_uri": REDIRECT_URI
    }

    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    response = requests.post(token_url, data=data, headers=headers)
    return response.json()

def get_user_info(access_token):
    userinfo_url = f"{COGNITO_DOMAIN}/oauth2/userInfo"
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get(userinfo_url, headers=headers)
    return response.json()

# ==============================
# USER DATABASE MANAGEMENT
# ==============================

def get_user_databases(email):
    response = table.get_item(Key={"email": email})
    if "Item" in response:
        return response["Item"].get("databases", [])
    else:
        table.put_item(Item={"email": email, "databases": []})
        return []

def add_database_to_user(email, db_name):
    response = table.get_item(Key={"email": email})
    dbs = response["Item"].get("databases", []) if "Item" in response else []

    if db_name not in dbs:
        dbs.append(db_name)

    table.put_item(Item={"email": email, "databases": dbs})

def remove_database_from_user(email, db_name):
    response = table.get_item(Key={"email": email})
    dbs = response["Item"].get("databases", []) if "Item" in response else []

    if db_name in dbs:
        dbs.remove(db_name)

    table.put_item(Item={"email": email, "databases": dbs})

def create_athena_database(db_name):
    query = f"CREATE DATABASE IF NOT EXISTS {db_name}"
    return execute_admin_query(query)

def delete_athena_database(db_name):
    query = f"DROP DATABASE IF EXISTS {db_name} CASCADE"
    return execute_admin_query(query)

def execute_admin_query(query):
    response = athena.start_query_execution(
        QueryString=query,
        ResultConfiguration={"OutputLocation": S3_OUTPUT},
    )

    execution_id = response["QueryExecutionId"]

    while True:
        status = athena.get_query_execution(QueryExecutionId=execution_id)
        state = status["QueryExecution"]["Status"]["State"]
        if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            break

    return state == "SUCCEEDED"

# ==============================
# QUERY FUNCTIONS
# ==============================

def run_query(query, database, kill_after_seconds=25):

    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": S3_OUTPUT},
    )

    execution_id = response["QueryExecutionId"]
    start_time = time.time()

    while True:
        status = athena.get_query_execution(QueryExecutionId=execution_id)
        state = status["QueryExecution"]["Status"]["State"]

        elapsed = time.time() - start_time

        # 🔥 Auto Kill After 25 Seconds
        if elapsed > kill_after_seconds and state == "RUNNING":
            athena.stop_query_execution(QueryExecutionId=execution_id)
            return None, "Query automatically stopped (Exceeded 25 seconds)."

        if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            break

        time.sleep(1)

    if state == "SUCCEEDED":
        results = athena.get_query_results(QueryExecutionId=execution_id)

        columns = [
            col["Label"]
            for col in results["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]
        ]

        rows = results["ResultSet"]["Rows"][1:]

        data = []
        for row in rows:
            data.append([col.get("VarCharValue", "") for col in row["Data"]])

        df = pd.DataFrame(data, columns=columns)

        # 🔥 Duplicate column safe handling
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

        return df, None

    error = status["QueryExecution"]["Status"].get("StateChangeReason", "Query failed.")
    return None, error

def suggest_optimization(query):
    suggestions = []
    optimized = query

    if "SELECT *" in query.upper():
        suggestions.append("Avoid SELECT *. Specify columns.")
        optimized = query.replace("SELECT *", "SELECT column1, column2")

    if "WHERE" not in query.upper():
        suggestions.append("Consider adding WHERE clause.")

    if "LIMIT" not in query.upper():
        suggestions.append("Add LIMIT to reduce scanned data.")
        optimized += "\nLIMIT 100"

    return suggestions, optimized


# ==============================
# STREAMLIT UI
# ==============================

st.set_page_config(page_title="DBIQ Agent", layout="wide")

query_params = st.query_params
code = query_params.get("code")

if "user" not in st.session_state:
    st.session_state.user = None

if code:
    tokens = exchange_code_for_token(code)
    access_token = tokens.get("access_token")
    if access_token:
        user_info = get_user_info(access_token)
        st.session_state.user = user_info.get("email")

if not st.session_state.user:
    st.title("🔐 DBIQ Agent Login")
    if st.button("Login"):
        st.markdown(f"<meta http-equiv='refresh' content='0; url={get_login_url()}'>", unsafe_allow_html=True)
    st.stop()

user_email = st.session_state.user

st.sidebar.write(f"Logged in as {user_email}")

if st.sidebar.button("Logout"):
    st.session_state.clear()
    st.markdown(f"<meta http-equiv='refresh' content='0; url={get_logout_url()}'>", unsafe_allow_html=True)
    st.stop()

st.title("🚀 DBIQ Agent - Multi User Dashboard")

# ==============================
# CREATE DATABASE
# ==============================

st.subheader("🆕 Create New Database")
new_db = st.text_input("Database Name")

if st.button("Create Database"):
    if new_db:
        safe_name = new_db.lower().replace(" ", "_")
        with st.spinner("Creating..."):
            success = create_athena_database(safe_name)

        if success:
            add_database_to_user(user_email, safe_name)
            st.success("Database created!")
            st.rerun()
        else:
            st.error("Failed to create database.")

# ==============================
# DELETE DATABASE
# ==============================

st.subheader("🗑 Delete Database")
databases = get_user_databases(user_email)

if databases:
    delete_db = st.selectbox("Select Database to Delete", databases)

    if st.button("Delete Selected Database"):
        with st.spinner("Deleting..."):
            success = delete_athena_database(delete_db)

        if success:
            remove_database_from_user(user_email, delete_db)
            st.success("Database deleted successfully!")
            st.rerun()
        else:
            st.error("Failed to delete database.")
else:
    st.warning("No databases assigned.")

# ==============================
# QUERY SECTION
# ==============================

databases = get_user_databases(user_email)

if not databases:
    st.stop()

selected_db = st.selectbox("Select Database", databases)

st.subheader("📝 Query Editor")
query = st.text_area("Enter SQL Query")


if st.button("Run Query"):
    if query:
        with st.spinner("Running query..."):
            result, error = run_query(query, selected_db)

        if error:
            st.error(error)
        else:
            st.success("Query Executed Successfully")
            st.dataframe(result, use_container_width=True)

        # 🔥 Always show suggestions
        suggestions, optimized = suggest_optimization(query)

        st.subheader("⚡ Optimization Suggestions")
        for s in suggestions:
            st.write("•", s)

        st.subheader("✨ Suggested Query")
        st.code(optimized, language="sql")
# ==============================
# LAST 100 QUERY DASHBOARD
# ==============================

st.markdown("---")
st.header("📊 Last 100 Query Monitoring Dashboard")

recent_df = get_recent_queries()

if not recent_df.empty:

    col1, col2, col3 = st.columns(3)

    col1.metric("Total Queries", len(recent_df))
    col2.metric("Failed Queries", len(recent_df[recent_df["Status"] == "FAILED"]))
    col3.metric("Total Cost (USD)", round(recent_df["Cost_USD"].sum(), 4))

    st.subheader("💰 Cost Trend")
    st.line_chart(recent_df["Cost_USD"])

    st.subheader("⏱ Execution Time Trend")
    st.bar_chart(recent_df["ExecutionTime_ms"])

    st.subheader("📄 Detailed Query Records")

    display_df = recent_df.rename(columns={
        "QueryID": "Query ID",
        "Database": "Database",
        "Tables_Used": "Tables Used",
        "Status": "Status",
        "ExecutionTime_ms": "Execution Time (ms)",
        "DataScanned_MB": "Data Retrieved (MB)",
        "Cost_USD": "Cost (USD)"
    })

    st.dataframe(display_df, use_container_width=True)

else:
    st.info("No recent query records found.")