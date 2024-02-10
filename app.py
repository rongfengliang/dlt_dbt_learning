import dlt

# have data? dlt likes data
data = [{'id': 1, 'name': 'John','age':111}, {'id': 2, 'name': 'Jane'}]

# open connection
pipeline = dlt.pipeline(
    pipeline_name="dalong",
    destination='postgres',
    dataset_name='postgres_data'
)

# Upsert/merge: Update old records, insert new
load_info = pipeline.run(
    data,
    write_disposition="merge",
    primary_key="id",
    table_name="users"
)

print(load_info)

pipeline = dlt.pipeline(
    pipeline_name='dalong',
    destination='postgres',
    dataset_name='postgres_data_dbt'
)

venv = dlt.dbt.get_venv(pipeline)


dbt = dlt.dbt.package(
    pipeline,
    "mydlt_dbt",
    venv=venv
)

models = dbt.run_all()

# on success print outcome
for m in models:
    print(
        f"Model {m.model_name} materialized" +
        f"in {m.time}" +
        f"with status {m.status}" +
        f"and message {m.message}"
    )