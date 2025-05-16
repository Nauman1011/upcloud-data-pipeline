import pandas as pd
import requests
from io import StringIO
from google.cloud import bigquery
from datetime import datetime, timedelta,date

def load_data_to_staging():
    # -- config --
    path_to_service_keys = r"C:/nifi-1.28.1-bin/nifi-1.28.1/keys/dbt-test-450313-55333885ed81.json"
    project_id = "dbt-test-450313"
    dataset_id = "dbt_nhayat"
    final_table_id = "billing_data_fact"
    staging_table_id = "billing_data_staging"

    full_staging_id = f"{project_id}.{dataset_id}.{staging_table_id}"
    full_final_id = f"{project_id}.{dataset_id}.{final_table_id}"

    bq_client = bigquery.Client.from_service_account_json(path_to_service_keys)

    # --Get latest date from final table --
    query = f"""
        SELECT MAX(DATE(billing_date)) AS max_date
        FROM `{project_id}.{dataset_id}.{final_table_id}`
    """
    max_date_result = bq_client.query(query).result().to_dataframe()
    max_date = max_date_result["max_date"][0]

    if pd.isna(max_date):
        # default to a start date if final table is empty
        max_date = datetime(2025, 4, 1).date()
        next_date = max_date
    else:
        next_date = max_date + timedelta(days=1)
    year, month, day = next_date.year, next_date.month, next_date.day


    # --constructing dynamic url --
    url = f"https://f7r4a.upcloudobjects.com/dwh-fina/billing/year={year}/month={month:02d}/day={day:02d}/billing.csv"
    filename = f"billing_{year}_{month:02d}_{day:02d}.csv"
    print(filename)
    print(f"Downloading: {url}")



    # --download and load csv --
    response = requests.get(url)
    response.raise_for_status()
    csv_data = StringIO(response.text)
    df = pd.read_csv(csv_data, dtype=str)
    # --Adding some additional columns that will be used to make sure data loaded correctly
    df["billing_date"] = next_date.strftime("%Y-%m-%d")
    df["data_loaded"] = "N"
    df["refresh_timestamp"] = datetime.utcnow().isoformat()

    # --Load to staging by truncating first--

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=False,
        schema=[bigquery.SchemaField(name, "STRING") for name in df.columns]
    )

    job = bq_client.load_table_from_dataframe(df, full_staging_id, job_config=job_config)
    job.result()  # Wait for the job to complete

    print(f"Loaded {df.shape[0]} rows into {full_staging_id}")

def load_data_to_final():
    # -- config --
    path_to_service_keys = r"C:/nifi-1.28.1-bin/nifi-1.28.1/keys/dbt-test-450313-55333885ed81.json"
    project_id = "dbt-test-450313"
    dataset_id = "dbt_nhayat"
    final_table_id = "billing_data_fact"
    staging_table_id = "billing_data_staging"

    full_staging_id = f"{project_id}.{dataset_id}.{staging_table_id}"
    full_final_id = f"{project_id}.{dataset_id}.{final_table_id}"

    bq_client = bigquery.Client.from_service_account_json(path_to_service_keys)

    # --We are doing delete insert to make sure there is no data overlap--
    delete_query = f"""
        DELETE FROM `{full_final_id}` AS t
        WHERE EXISTS (
            SELECT 1
            FROM `{full_staging_id}` AS r
            WHERE t.billing_date = CAST(r.billing_date AS DATE)
        )
    """
    bq_client.query(delete_query).result()
    print("Deleted overlapping records from fact table.")

    # --Insert data from staging to final table and according to correct data types --
    insert_query = f"""
        INSERT INTO `{full_final_id}` (
            billing_timestamp,
            resource_id,
            user_id,
            credit_usage,
            region,
            service_tier,
            operation_type,
            success,
            resource_type,
            invoice_id,
            currency,
            billing_date
        )
        SELECT
            CAST(timestamp AS TIMESTAMP) AS billing_timestamp,
            resource_id,
            user_id,
            CAST(credit_usage AS FLOAT64) AS credit_usage,
            region,
            service_tier,
            operation_type,
            CAST(success AS BOOL) AS success,
            resource_type,
            invoice_id,
            currency,
            CAST(billing_date AS DATE) AS billing_date
        FROM `{full_staging_id}`
        WHERE data_loaded = 'N'
    """
    bq_client.query(insert_query).result()
    print("Inserted new records into fact table.")

    #mark records in staging as loaded
    update_query = f"""
        UPDATE `{full_staging_id}`
        SET data_loaded = 'Y'
        WHERE data_loaded = 'N'
    """
    bq_client.query(update_query).result()
    print("Updated staging table to mark records as loaded.")



start_date = date(2025, 4, 1)
end_date = date(2025, 4, 30)

current = start_date
while current <= end_date:
    load_data_to_staging()
    load_data_to_final()
    current += timedelta(days=1)