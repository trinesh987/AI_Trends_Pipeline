from dagster import (
    Definitions,
    EnvVar,
    ScheduleDefinition,
    asset,
    job,
    op,
    resource,
    AssetExecutionContext,
    MetadataValue,
)
from google.cloud import bigquery
import pandas as pd
import requests
import os
from datetime import datetime,timedelta
import time

@resource (config_schema= {"gcp_cred_path":str})
def bigquery_resource(context):
    return bigquery.Client.from_service_account_json(
        context.resource_config["gcp_cred_path"]
    )

@op(required_resource_keys={"bq"})
def fetch_articles(context) -> pd.DataFrame:
    client = context.resources.bq
    query = "SELECT initial_run_done FROM `clear-column-447115-s3.ai_trends.pipeline_metadata` where rand() is not null limit 1"
    result = client.query(query).result()
    row = next(result, None)
    if row:
         context.log.info(f"initial_run_done value ={row.initial_run_done}")
    else:
         context.log.info("No row found in pipeline metadata")
    if row and row.initial_run_done:  
            from_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")  # Normal daily load
    else:
            from_date = "2023-01-01" 
            
    to_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    context.log.info(f"fetching data from {from_date} to {to_date}")

    if not(row and row.initial_run_done):
         client.query("UPDATE `clear-column-447115-s3.ai_trends.pipeline_metadata` SET initial_run_done = True WHERE TRUE").result()
         context.log.info("Initial run completed.Flag updated to True")




    api_key = os.getenv("GUARDIAN_API_KEY")
    if not api_key:
        raise ValueError("GUARDIAN_API_KEY not found in environmental variables")
    
    url ="https://content.guardianapis.com/search"
    params = {
        "q" : "AI OR ChatGPT OR Machine Learning",
        "from-date" : from_date,
        "to-date" : to_date,
        "page-size":50,
        "page": 1,
        "api-key": api_key

    }
    all_articles =[]
    while True:
        response = requests.get(url,params=params,verify=False)
        data = response.json()

        if data['response']['status'] != "ok" or not data['response']['results']:
            break
        
        all_articles.extend([
            {
                "Title" : article.get("webTitle"),
                "Publication Date" : article.get("webPublicationDate"),
                "Section": article.get("sectionName"),
                "URL": article.get("webUrl")

            } for article in data['response']['results']
        ])

        if params["page"] >= data['response']['pages']:
            break
        params['page'] += 1
        time.sleep(1)
    df = pd.DataFrame(all_articles)
    context.log.info(f"fetched {len(df)} articles from {from_date} to {to_date}")
    return df

@op
def transform_articles(context,df: pd.DataFrame) -> pd.DataFrame:

    if df.empty:
        context.log.info("No articles to transform")
        return df
    
    df['Publication Date'] = pd.to_datetime(df['Publication Date'] , errors='coerce')
    df = df.dropna(subset=['Publication Date'])

    df['Year'] = df['Publication Date'].dt.year
    df['Month'] = df['Publication Date'].dt.month
    replacements = {
        r'â€˜': '‘',
        r'â€™': '’',
        r'â€“': '–',
        r'â€”': '—',
        r'â€œ': '“',
        r'â€': '”'
    }
    for wrong, correct in replacements.items():
                df['Title'] = df['Title'].astype(str).str.replace(wrong, correct, regex=True)
    return df
@op (required_resource_keys= {"bq"})
def load_to_bigquery(context,df:pd.DataFrame):
    if df.empty:
        context.log.info("No data to load")
        return
    client = context.resources.bq
    table_ref = "clear-column-447115-s3.ai_trends.guardian_articles"
    job_config = bigquery.LoadJobConfig(
         write_disposition = "WRITE_APPEND"    
        )
    job = client.load_table_from_dataframe(
        df.rename(columns={'Publication Date': 'Publication_Date'}),
        table_ref,
        job_config=job_config
    )
    job.result()
    context.log.info(f"Loaded {len(df)} rows")

@job(resource_defs={"bq":bigquery_resource.configured({"gcp_cred_path":"gcp.json"})})
def guardian_ingestion_job():
     data = fetch_articles()
     transformed= transform_articles(data)
     load_to_bigquery(transformed)


guardian_schedule = ScheduleDefinition(
     job=guardian_ingestion_job,
     cron_schedule="0 8 * * *"
)

defs = Definitions(
     jobs=[guardian_ingestion_job],
    schedules=[guardian_schedule]
)