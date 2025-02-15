# Homework: Data Ingestion with dlt

## Quiz Questions

### Question 1: dlt Version
```python
!dlt --version
```
__Answer:__
- `dlt 1.6.1`


### Question 2: Define & Run the Pipeline (NYC Taxi API)  
__How many tables were created?__
```python
import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator
import duckdb
from google.colab import data_table
```
```python
@dlt.resource(name="rides")
def ny_taxi():
    client = RESTClient(
        base_url="https://us-central1-dlthub-analytics.cloudfunctions.net",
        paginator=PageNumberPaginator(
            base_page=1,
            total_path=None
        )
    )

    for page in client.paginate("data_engineering_zoomcamp_api"):
        yield page

pipeline = dlt.pipeline(
    pipeline_name="ny_taxi_pipeline",
    destination="duckdb",
    dataset_name="ny_taxi_data"
)
```
```python
data_table.enable_dataframe_formatter()
conn = duckdb.connect(f"{pipeline.pipeline_name}.duckdb")
conn.sql(f"SET search_path = '{pipeline.dataset_name}'")
conn.sql("DESCRIBE").df()
```
__Answer:__
- `4`


### Question 3: Explore the loaded data  
__What is the total number of records extracted?__
```python
df = pipeline.dataset(dataset_type="default").rides.df()
df.shape
```
__Answer:__
- `10000`


### Question 4: Trip Duration Analysis  
__What is the average trip duration?__
```python
with pipeline.sql_client() as client:
    res = client.execute_sql(
            """
            SELECT
            AVG(date_diff('minute', trip_pickup_date_time, trip_dropoff_date_time))
            FROM rides;
            """
        )
    print(res)
```
__Answer:__
- `12.3049`
