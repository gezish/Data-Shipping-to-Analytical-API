from dagster import job, op
import subprocess

@op
def scrape_op(context):
    context.log.info("Running scraper...")
    subprocess.run(["python", "telegram_scraper.py", "--channels-file", "channels.txt", "--incremental"])

@op
def load_op(context):
    context.log.info("Loading to Postgres...")
    subprocess.run(["python", "load_raw_to_postgres.py"])

@op
def dbt_op(context):
    context.log.info("Running dbt models...")
    subprocess.run(["dbt", "run"], check=False)

@op
def yolo_op(context):
    context.log.info("Running YOLO enrichment...")
    subprocess.run(["python", "yolo_enrich.py"])

@job
def telegram_pipeline():
    scrape_op()
    load_op()
    dbt_op()
    yolo_op()
