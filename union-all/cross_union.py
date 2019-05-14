from google.cloud import bigquery

client = bigquery.Client()
dataset_id = 'union'

client.create_dataset(dataset_id, exists_ok=True)

job_config = bigquery.QueryJobConfig()
# Set the destination table
table_ref = client.dataset(dataset_id).table('loan')
client.delete_table(table_ref, not_found_ok=True)
job_config.destination = table_ref
sql = """
    SELECT *
    FROM `fifty-shades-of-brown.know_when_you_are_beaten.loan_1`
    UNION ALL
    (SELECT * FROM
    `i-love-brownies-3000.thanos.loan_2`
    );
"""

# Start the query, passing in the extra configuration.
query_job = client.query(
    sql,
    # Location must match that of the dataset(s) referenced in the query
    # and of the destination table.
    location='US',
    job_config=job_config)  # API request - starts the query

query_job.result()  # Waits for the query to finish
print('Query results loaded to table {}'.format(table_ref.path))
