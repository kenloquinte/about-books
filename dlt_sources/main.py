import dlt

# data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

# pipeline = dlt.pipeline(
#     pipeline_name="sample", destination="duckdb", dataset_name="sampledata"
# )

# load_info = pipeline.run(data, table_name="users")

# print(load_info)

from dlt.sources.helpers import requests

url = 'https://openlibrary.org/search/inside.json?q="love"'

response = requests.get(url)
response.raise_for_status()

pipeline = dlt.pipeline(
    pipeline_name="openlibrary_pipeline",
    destination="duckdb",
    dataset_name="openlibrary_books",
)
# The response contains a list of issues
load_info = pipeline.run(response.json()['hits']['hits'], table_name="love")

print(load_info)