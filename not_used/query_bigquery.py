from google.cloud import bigquery

def query_shakespeare():
    client = bigquery.Client()
    query_results = client.run_sync_query("""
        SELECT
            COUNT(*)
        FROM `amazon_data.metadata`
        WHERE metadata.asin =='B00JAPDUJO';""")

    # Use standard SQL syntax for queries.
    # See: https://cloud.google.com/bigquery/sql-reference/
    query_results.use_legacy_sql = False

    query_results.run()

    # Drain the query results by requesting a page at a time.
    page_token = None

    while True:
        rows, total_rows, page_token = query_results.fetch_data(
            max_results=10,
            page_token=page_token)

        for row in rows:
            print(row)

        if not page_token:
            break


if __name__ == '__main__':
    query_shakespeare()