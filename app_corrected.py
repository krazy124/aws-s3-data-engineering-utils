import importlib
import json
import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st
from ui_helpers import run_ui_safely

PROJECT_ROOT = Path(__file__).resolve().parent
SRC_DIR = PROJECT_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

s3 = importlib.import_module("s3_operations")
glue = importlib.import_module("glue_crawler_operations")
athena = importlib.import_module("athena_operations")
monsterforge = importlib.import_module("monsterforge_etl")

st.set_page_config(
    page_title="MonsterForge Data Engineering Lab",
    page_icon="🧪",
    layout="wide",
)

BUCKET_NAME = "wlmdatawizard-monsterforge-873851887650"
REGION = "us-east-1"
DATABASE_NAME = "monsterforge_data_lake"
ATHENA_OUTPUT = f"s3://{BUCKET_NAME}/athena-results/"

CLEAN_CRAWLER = "monsterforge-clean-crawler"
QUARANTINE_CRAWLER = "monsterforge-quarantine-crawler"


def init_session_state():
    defaults = {
        "page": "Home",
        "run_id": None,
        "raw_s3_key": None,
        "clean_s3_key": None,
        "quarantine_s3_key": None,
        "report_s3_key": None,
        "etl_report": None,
    }

    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def go_to(page_name):
    st.session_state.page = page_name
    st.rerun()


def generate_run_id():
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def save_uploaded_file(uploaded_file, suffix=".csv"):
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        tmp.write(uploaded_file.getbuffer())
        return tmp.name


def write_text_temp_file(text, suffix):
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix, mode="w", encoding="utf-8") as tmp:
        tmp.write(text)
        return tmp.name


def upload_local_file_to_s3(local_path, s3_key):
    return s3.upload_file(
        bucket_name=BUCKET_NAME,
        file_path=local_path,
        object_name=s3_key,
        region=REGION,
    )


def upload_dataframe_to_s3(df, s3_key):
    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv", mode="w", encoding="utf-8", newline="") as tmp:
        df.to_csv(tmp.name, index=False)
        temp_path = tmp.name

    result = upload_local_file_to_s3(temp_path, s3_key)
    os.remove(temp_path)
    return result


def upload_json_to_s3(data, s3_key):
    temp_path = write_text_temp_file(json.dumps(data, indent=2, default=str), ".json")
    result = upload_local_file_to_s3(temp_path, s3_key)
    os.remove(temp_path)
    return result


def show_pipeline_status():
    st.subheader("Pipeline Status")

    col1, col2, col3, col4 = st.columns(4)

    col1.metric("Run ID", st.session_state.run_id or "Not started")
    col2.metric("Raw Upload", "✅" if st.session_state.raw_s3_key else "❌")
    col3.metric("ETL Output", "✅" if st.session_state.clean_s3_key else "❌")
    col4.metric("Report", "✅" if st.session_state.report_s3_key else "❌")

    if st.session_state.run_id:
        with st.expander("Current run paths"):
            st.write("Raw:", st.session_state.raw_s3_key)
            st.write("Clean:", st.session_state.clean_s3_key)
            st.write("Quarantine:", st.session_state.quarantine_s3_key)
            st.write("Report:", st.session_state.report_s3_key)


def show_home_page():
    st.title("🧪 MonsterForge Data Engineering Lab")

    st.markdown(
        """
        A portfolio data engineering pipeline using:

        **Python • PySpark • Amazon S3 • AWS Glue • Athena • Streamlit**

        This app demonstrates how raw monster manufacturing data moves through a data lake,
        gets cleaned, quarantined, cataloged, queried, and displayed.
        """
    )

    show_pipeline_status()

    st.divider()

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.subheader("📦 S3 Upload")
        st.write("Upload raw MonsterForge CSV files into the data lake.")
        if st.button("Open S3 Tools", use_container_width=True):
            go_to("S3 Tools")

    with col2:
        st.subheader("🧬 ETL Pipeline")
        st.write("Run PySpark transformations and create clean/quarantine outputs.")
        if st.button("Open ETL Pipeline", use_container_width=True):
            go_to("MonsterForge ETL")

    with col3:
        st.subheader("🕷️ Glue Crawlers")
        st.write("Catalog S3 data so Athena can query it.")
        if st.button("Open Glue Crawlers", use_container_width=True):
            go_to("Glue Crawlers")

    with col4:
        st.subheader("🔎 Athena")
        st.write("Preview tables, count records, and run SQL queries.")
        if st.button("Open Athena Dashboard", use_container_width=True):
            go_to("Athena Dashboard")

    st.divider()

    st.subheader("Recommended Workflow")

    st.markdown(
        """
        1. Upload raw MonsterForge CSV to S3  
        2. Run the PySpark ETL pipeline  
        3. Upload clean, quarantine, and report outputs to S3  
        4. Run Glue crawlers  
        5. Query the cataloged data with Athena  
        """
    )


def show_s3_tools_page():
    st.title("📦 S3 Tools: Upload Raw MonsterForge Data")

    st.info(
        "This page uploads raw MonsterForge CSV files into a run-based S3 path."
    )

    uploaded_file = st.file_uploader(
        "Choose raw MonsterForge CSV",
        type=["csv"],
    )

    if uploaded_file is None:
        st.warning("Upload a CSV file to begin.")
        return

    st.success(f"Selected file: {uploaded_file.name}")

    with st.expander("Preview uploaded file"):
        preview_df = pd.read_csv(uploaded_file)
        st.dataframe(preview_df.head(25), use_container_width=True)
        uploaded_file.seek(0)

    if st.button("Upload Raw File to S3", type="primary"):
        run_id = generate_run_id()

        raw_key = f"raw/monsters/runs/run_id={run_id}/monsters.csv"
        latest_key = "raw/monsters/latest/monsters.csv"

        temp_path = save_uploaded_file(uploaded_file)

        raw_uploaded = upload_local_file_to_s3(temp_path, raw_key)
        latest_uploaded = upload_local_file_to_s3(temp_path, latest_key)

        os.remove(temp_path)

        if raw_uploaded and latest_uploaded:
            st.session_state.run_id = run_id
            st.session_state.raw_s3_key = raw_key

            st.success("Raw file uploaded successfully.")
            st.code(f"s3://{BUCKET_NAME}/{raw_key}")
            st.caption("Also updated latest raw file:")
            st.code(f"s3://{BUCKET_NAME}/{latest_key}")
        else:
            st.error("Upload failed.")


def show_etl_page():
    st.title("🧬 MonsterForge ETL Pipeline")

    st.info(
        "This page runs the MonsterForge PySpark cleaning pipeline locally, "
        "then uploads clean, quarantine, and report outputs to S3."
    )

    uploaded_file = st.file_uploader(
        "Choose raw MonsterForge CSV for ETL",
        type=["csv"],
        key="etl_upload",
    )

    if uploaded_file is None:
        st.warning("Upload a raw MonsterForge CSV before running ETL.")
        return

    if st.button("Run MonsterForge ETL", type="primary"):
        try:
            from pyspark.sql import SparkSession
            from monsterforge_etl import (
                load_monsterforge_raw_data,
                monsterforge_cleaning_pipeline,
                split_clean_quarantine,
                generate_quality_report,
            )

            run_id = st.session_state.run_id or generate_run_id()
            temp_input_path = save_uploaded_file(uploaded_file)

            spark = (
                SparkSession.builder
                .appName("MonsterForgeStreamlitETL")
                .master("local[*]")
                .getOrCreate()
            )

            with st.spinner("Loading raw data with PySpark..."):
                raw_df = load_monsterforge_raw_data(spark, temp_input_path)

            if raw_df is None:
                st.error("Raw data failed to load.")
                os.remove(temp_input_path)
                return

            with st.spinner("Running MonsterForge cleaning pipeline..."):
                cleaned_df = monsterforge_cleaning_pipeline(raw_df)
                clean_df, quarantine_df = split_clean_quarantine(cleaned_df)
                report = generate_quality_report(cleaned_df, clean_df, quarantine_df)

            clean_pd = clean_df.toPandas()
            quarantine_pd = quarantine_df.toPandas()

            clean_key = f"clean/monsters/runs/run_id={run_id}/monsters_clean.csv"
            quarantine_key = f"quarantine/monsters/runs/run_id={run_id}/monsters_quarantine.csv"
            report_key = f"reports/monsters/runs/run_id={run_id}/quality_report.json"

            latest_clean_key = "clean/monsters/latest/monsters_clean.csv"
            latest_quarantine_key = "quarantine/monsters/latest/monsters_quarantine.csv"
            latest_report_key = "reports/monsters/latest/quality_report.json"

            table_clean_key = "clean/monsters/table/monsters_clean.csv"
            table_quarantine_key = "quarantine/monsters/table/monsters_quarantine.csv"

            with st.spinner("Uploading ETL outputs to S3..."):
                clean_uploaded = upload_dataframe_to_s3(clean_pd, clean_key)
                quarantine_uploaded = upload_dataframe_to_s3(quarantine_pd, quarantine_key)
                report_uploaded = upload_json_to_s3(report, report_key)

                latest_clean_uploaded = upload_dataframe_to_s3(clean_pd, latest_clean_key)
                latest_quarantine_uploaded = upload_dataframe_to_s3(quarantine_pd, latest_quarantine_key)
                latest_report_uploaded = upload_json_to_s3(report, latest_report_key)

                table_clean_uploaded = upload_dataframe_to_s3(clean_pd, table_clean_key)
                table_quarantine_uploaded = upload_dataframe_to_s3(
                    quarantine_pd,
                    table_quarantine_key,
                )

            os.remove(temp_input_path)

            if all(
                [
                    clean_uploaded,
                    quarantine_uploaded,
                    report_uploaded,
                    latest_clean_uploaded,
                    latest_quarantine_uploaded,
                    latest_report_uploaded,
                    table_clean_uploaded,
                    table_quarantine_uploaded,
                ]
            ):
                st.session_state.run_id = run_id
                st.session_state.clean_s3_key = table_clean_key
                st.session_state.quarantine_s3_key = table_quarantine_key
                st.session_state.report_s3_key = report_key
                st.session_state.etl_report = report

                st.success("ETL completed and outputs uploaded to S3.")

                col1, col2, col3 = st.columns(3)
                col1.metric("Total Records", report["total_records"])
                col2.metric("Clean Records", report["clean_records"])
                col3.metric("Quarantined Records", report["quarantined_records"])

                st.subheader("Clean Data Preview")
                st.dataframe(clean_pd.head(25), use_container_width=True)

                st.subheader("Quarantine Data Preview")
                st.dataframe(quarantine_pd.head(25), use_container_width=True)

                st.subheader("Quality Report")
                st.json(report)

            else:
                st.error("ETL ran, but one or more uploads failed.")

        except ModuleNotFoundError as exc:
            st.error("PySpark or ETL modules are not available in this environment.")
            st.exception(exc)

        except Exception as exc:
            st.error("MonsterForge ETL failed.")
            st.exception(exc)


def show_glue_page():
    st.title("🕷️ Glue Crawlers")

    st.info(
        "Glue crawlers scan the clean and quarantine folders in S3 and update the Glue Data Catalog."
    )

    crawlers = glue.list_glue_crawlers(region=REGION)

    if not crawlers:
        st.warning("No Glue crawlers found.")
        return

    selected_crawler = st.selectbox(
        "Select crawler",
        crawlers,
        index=crawlers.index(CLEAN_CRAWLER) if CLEAN_CRAWLER in crawlers else 0,
    )

    details = glue.get_crawler_details(selected_crawler, region=REGION)

    if details:
        st.subheader("Crawler Details")
        st.json(details)

    status = glue.get_crawler_status(selected_crawler, region=REGION)
    st.metric("Current Status", status or "Unknown")

    col1, col2 = st.columns(2)

    with col1:
        if st.button("Start Selected Crawler", type="primary"):
            started = glue.start_crawler(selected_crawler, region=REGION)

            if started:
                st.success(f"Started crawler: {selected_crawler}")
            else:
                st.error("Crawler did not start. It may already be running.")

    with col2:
        if st.button("Wait For Selected Crawler"):
            with st.spinner("Waiting for crawler to finish..."):
                finished = glue.wait_for_crawler(
                    selected_crawler,
                    region=REGION,
                    delay=5,
                    max_attempts=60,
                )

            if finished:
                st.success("Crawler finished.")
            else:
                st.error("Crawler timed out or failed.")

    st.divider()

    st.subheader("Quick Actions")

    col3, col4 = st.columns(2)

    with col3:
        if st.button("Run Clean Crawler", use_container_width=True):
            result = glue.start_crawler(CLEAN_CRAWLER, region=REGION)
            st.success("Clean crawler started.") if result else st.error("Clean crawler did not start.")

    with col4:
        if st.button("Run Quarantine Crawler", use_container_width=True):
            result = glue.start_crawler(QUARANTINE_CRAWLER, region=REGION)
            st.success("Quarantine crawler started.") if result else st.error("Quarantine crawler did not start.")


def show_athena_page():
    st.title("🔎 Athena Dashboard")

    st.info(
        "Athena queries tables created in the Glue Data Catalog."
    )

    tables = athena.show_tables(
        database=DATABASE_NAME,
        output_location=ATHENA_OUTPUT,
        region=REGION,
    )

    if not tables:
        st.warning(
            "No Athena tables found. Upload data and run Glue crawlers first."
        )
        return

    selected_table = st.selectbox("Select table", tables)

    col1, col2, col3 = st.columns(3)

    with col1:
        if st.button("Preview Table", use_container_width=True):
            rows = athena.preview_table(
                table_name=selected_table,
                database=DATABASE_NAME,
                output_location=ATHENA_OUTPUT,
                limit=25,
                region=REGION,
            )

            if rows:
                st.dataframe(pd.DataFrame(rows), use_container_width=True)
            else:
                st.warning("No preview rows returned.")

    with col2:
        if st.button("Count Rows", use_container_width=True):
            rows = athena.count_table_rows(
                table_name=selected_table,
                database=DATABASE_NAME,
                output_location=ATHENA_OUTPUT,
                region=REGION,
            )

            if rows:
                st.dataframe(pd.DataFrame(rows), use_container_width=True)
            else:
                st.warning("No count returned.")

    with col3:
        if st.button("Describe Table", use_container_width=True):
            rows = athena.describe_table(
                table_name=selected_table,
                database=DATABASE_NAME,
                output_location=ATHENA_OUTPUT,
                region=REGION,
            )

            if rows:
                st.dataframe(pd.DataFrame(rows), use_container_width=True)
            else:
                st.warning("No schema returned.")

    st.divider()

    st.subheader("Custom SQL Query")

    default_query = f"SELECT * FROM {selected_table} LIMIT 25"

    query = st.text_area(
        "SQL",
        value=default_query,
        height=160,
    )

    if st.button("Run Query", type="primary"):
        rows = athena.run_query(
            query=query,
            database=DATABASE_NAME,
            output_location=ATHENA_OUTPUT,
            region=REGION,
        )

        if rows:
            st.dataframe(pd.DataFrame(rows), use_container_width=True)
        else:
            st.warning("Query returned no rows or failed.")


def show_s3_browser_page():
    """Display S3 browser page."""

    st.header("📂 S3 Browser")

    prefix = st.text_input(
        "Folder Prefix (optional)",
        value="",
        help="Example: raw/monsters/ or clean/monsters/"
    )

    try:
        if prefix:
            files = s3.list_files_in_prefix(
                bucket_name=BUCKET_NAME,
                prefix=prefix,
                region=REGION
            )
        else:
            files = s3.list_files(
                bucket_name=BUCKET_NAME,
                region=REGION
            )

        if files:
            st.success(f"Found {len(files)} file(s)")

            file_df = pd.DataFrame({
                "File Path": files
            })

            st.dataframe(
                file_df,
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("No files found.")

    except Exception as e:
        st.error(f"Error loading files: {e}")


init_session_state()

pages = [
    "Home",
    "S3 Tools",
    "MonsterForge ETL",
    "Glue Crawlers",
    "Athena Dashboard",
    "S3 Browser",
]

st.sidebar.radio(
    "Navigation",
    pages,
    key="page",
)

st.sidebar.divider()
st.sidebar.caption("MonsterForge AWS Pipeline")
st.sidebar.caption(f"Bucket: {BUCKET_NAME}")
st.sidebar.caption(f"Database: {DATABASE_NAME}")

if st.session_state.page == "Home":
    show_home_page()
elif st.session_state.page == "S3 Tools":
    show_s3_tools_page()
elif st.session_state.page == "MonsterForge ETL":
    show_etl_page()
elif st.session_state.page == "Glue Crawlers":
    show_glue_page()
elif st.session_state.page == "Athena Dashboard":
    show_athena_page()
elif st.session_state.page == "S3 Browser":
    show_s3_browser_page()
