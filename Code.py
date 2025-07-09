# Etl_Local.py:

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from snowflake.connector import connect
from datetime import datetime
import pandas as pd
default_args = {
'owner': 'Admin',
'depends_on_past': False,
'retries': 1,
}
dag = DAG(
dag_id='etl_local',
description='Load data into Snowflake from Local Storage',
schedule_interval=None,
start_date=datetime(2024, 1, 29),
default_args=default_args,
catchup=False,
)
def etl_function():
print("EXTRACTION STARTED")
extraction_returned_data = extract_data_from_csv()
print("TRANSFORMATION STARTED")
transformation_returned_data = transform_data(extraction_returned_data)
print("LOADING STARTED")
load_data_to_snowflake(transformation_returned_data)
print("ALL DONE")
def extract_data_from_csv():
local_csv_file = "data/hoteltariff.csv"
with open(local_csv_file, 'r') as file:
data = pd.read_csv(file)
return data
def find_most_common(lst):
count_dict = {} #{1500:2,1200:3}
for item in lst:
if item in count_dict:
count_dict[item] += 1
else:
count_dict[item] = 1
return max(count_dict, key=lambda x: count_dict[x])
def transform_data(extracted):
data_frame = extracted.copy()
tariff_columns = ['SundayTariff', 'MondayTariff', 'TuesdayTariff', 'WednesdayTariff', 'ThursdayTariff', 'FridayTariff', 'SaturdayTariff']
weekend = ['FridayTariff', 'SaturdayTariff', 'SundayTariff']
data_frame = data_frame.fillna(0)
for index, row in data_frame.iterrows():
row_values = row[tariff_columns]
zero_count = 0
non_zero_values = []
for value in row_values:
if value == 0:
zero_count += 1
elif value != 0:
non_zero_values.append(value)
modification_data_frame = data_frame.copy()
if zero_count < len(row_values):
for col in tariff_columns:
if row[col] == 0:
if col in weekend:
max_value = max(non_zero_values)
modification_data_frame.at[index, col] = max_value
else:
most_repeated_value = find_most_common(non_zero_values)
20
modification_data_frame.at[index, col] = most_repeated_value
return modification_data_frame
def load_data_to_snowflake(transformed):
snowflake_params = {
}
conn = connect(**snowflake_params)
cur = conn.cursor()
try:
transformed_data = transformed.copy()
snowflake_table = 'HotelTariff'
columns = transformed_data.columns.tolist()
drop_table_statement = f"DROP TABLE IF EXISTS {snowflake_table};"
cur.execute(drop_table_statement)
create_table_statement = f"CREATE TABLE {snowflake_table} ("
for col in columns:
if col in ['Hotel', 'City', 'District', 'Roomtype']:
create_table_statement += f"{col} VARCHAR(255),"
else:
create_table_statement += f"{col} INT,"
create_table_statement = create_table_statement.rstrip(',')
create_table_statement += ");"
cur.execute(create_table_statement)
for index, row in transformed_data.iterrows():
values = []
for value in row:
if isinstance(value, str):
values.append(f"'{value}'")
else:
values.append(str(value))
column_names = ', '.join(columns)
row_values = ', '.join(values)
sql_statement = f"INSERT INTO {snowflake_table} ({column_names}) VALUES ({row_values})"
21
print(f"SQL statement: {sql_statement}")
cur.execute(sql_statement)
conn.commit()
print("DATA LOADING SUCCESSFUL!!")
except Exception as e:
print(f"Error: {str(e)}")
conn.rollback()
finally:
cur.close()
conn.close()
extract_transform_load = PythonOperator(
task_id='extract_transform_load',
python_callable=etl_function,
dag=dag,
)
start = DummyOperator(task_id='start')
end = DummyOperator(task_id='end')
start >> extract_transform_load >> end

# Etl_Database:
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from snowflake.connector import connect
from datetime import datetime
import mysql.connector
import pandas as pd
dag_id = 'etl_database_to_snowflake_pipeline'
schedule_interval = None
default_args = {
'owner': 'Admin',
'start_date': datetime(2024, 1, 29),
'retries': 1,
}
dag = DAG(
dag_id=dag_id,
default_args=default_args,
schedule_interval=schedule_interval,
catchup=False,
max_active_runs=1,
)
22
def etl_function():
print("EXTRACTION STARTED")
extraction_returned_data = extract_data_from_csv()
print("TRANSFORMATION STARTED")
transformation_returned_data = transform_data(extraction_returned_data)
print("LOADING STARTED")
load_data_to_snowflake(transformation_returned_data)
print("ALL DONE")
def extract_data_from_csv():
mysql_params = {
}
conn = mysql.connector.connect(**mysql_params)
cursor = conn.cursor()
query = 'SELECT * FROM tourismandculture'
try:
cursor.execute(query)
data = cursor.fetchall()
columns = [desc[0] for desc in cursor.description]
extracted_data = pd.DataFrame(data, columns=columns)
return extracted_data
finally:
cursor.close()
conn.close()
def transform_data(extracted):
data = extracted.copy()
my_data = data.fillna(0)
return my_data
def load_data_to_snowflake(transformed):
snowflake_params = {
} 
conn = connect(**snowflake_params)
cur = conn.cursor()
try:
transformed_data = transformed.copy()
snowflake_table = 'tourismandculture'
columns = transformed_data.columns.tolist()
drop_table_statement = f"DROP TABLE IF EXISTS {snowflake_table};"
cur.execute(drop_table_statement)
create_table_statement = f"CREATE TABLE {snowflake_table} ("
for col in columns:
if col in ['District']:
create_table_statement += f"{col} VARCHAR(255),"
else:
create_table_statement += f"{col} INT,"
create_table_statement = create_table_statement.rstrip(',')
create_table_statement += ");"
cur.execute(create_table_statement)
for index, row in transformed_data.iterrows():
values = []
for value in row:
if isinstance(value, str):
values.append(f"'{value}'")
else:
values.append(str(value))
column_names = ', '.join(columns)
row_values = ', '.join(values)
sql_statement = f"INSERT INTO {snowflake_table} ({column_names}) VALUES ({row_values})"
cur.execute(sql_statement)
conn.commit()
print("DATA LOADING SUCCESSFUL!!")
except Exception as e:
print(f"Error: {str(e)}")
conn.rollback()
finally:
cur.close()

conn.close()
extract_transform_load = PythonOperator(
task_id='extract_transform_load',
python_callable=etl_function,
dag=dag,
)
start = DummyOperator(task_id='start')
end = DummyOperator(task_id='end')
start >> extract_transform_load >> end

# Etl_Domestic_Visitors:

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from snowflake.connector import connect
from datetime import datetime
import pandas as pd
import io
dag_id = 'etl_cloud_to_snowflake_domestic'
schedule_interval = None
default_args = {
'owner': 'Admin',
'start_date': datetime(2024, 1, 29),
'retries': 1,
}
dag = DAG(
dag_id=dag_id,
default_args=default_args,
schedule_interval=schedule_interval,
catchup=False,
max_active_runs=1,
)
s3_bucket_name = 'abc'
aws_access_key = 'xyz'
aws_secret_key = 'xyz'
aws_conn_id='xyz'
def etl_function():
print("EXTRACTION STARTED")
25
extraction_returned_data = extract_data_from_s3(s3_bucket_name, aws_conn_id)
print("TRANSFORMATION STARTED")
transformation_returned_data = transform_data(extraction_returned_data)
print("LOADING STARTED")
load_data_to_snowflake(transformation_returned_data)
print("ALL DONE")
def extract_data_from_s3(bucket_name, aws_conn_id):
try:
s3_hook = S3Hook(aws_conn_id)
s3_objects = s3_hook.list_keys(bucket_name=bucket_name)
if not s3_objects:
raise Exception(f"No objects found in the S3 bucket '{bucket_name}'.")
latest_object = max(s3_objects)
file_content = s3_hook.read_key(latest_object, bucket_name)
data_frame = pd.read_csv(io.StringIO(file_content))
print("EXTRACTION DONE")
return data_frame
except Exception as e:
print(f"Error in extract_data_from_s3: {str(e)}")
raise
def transform_data(extracted):
try:
data_frame = extracted.copy()
all_columns = data_frame.columns.tolist()
non_visitor_columns = ['District','Month']
visitor_columns=[]
for i in all_columns:
if i not in non_visitor_columns:
visitor_columns.append(i)
for col in visitor_columns: # 2016
new_col_values = [] # [7921,655,0,959696,0]
for value in data_frame[col]: # 7921
if pd.isna(value) or not value.strip().isdigit():
new_col_values.append(0)
else:
new_col_values.append(int(value))
data_frame[col] = new_col_values
district_sums = {} # {(adi,2016):24146,(hyd,2016):24146,(adi,2017):24146}
district_counts = {} # {(adi,2016):2,(hyd,2016):2,(adi,2017):2}
26
for index, row in data_frame.iterrows():
district = row['District'] # adilabad
for col in visitor_columns: # 2016
if row[col] != 0: # 9512
key = (district, col) # key = (adi,2016)
if key not in district_sums:
district_sums[key] = 0
district_counts[key] = 0
district_sums[key] += row[col]
district_counts[key] += 1
district_means = {} # {adi:{2016:24146//2,2017:2546//3}}
for (district, col), value_sum in district_sums.items():
count = district_counts[(district, col)]
if district not in district_means:
district_means[district] = {}
if count > 0:
district_means[district][col] = value_sum // count
else:
district_means[district][col] = 0
modified_data_frame = data_frame.copy()
for index, row in data_frame.iterrows():
district = row['District']
for col in visitor_columns:
if row[col] == 0:
if district in district_means and col in district_means[district]:
modified_data_frame.at[index, col] = district_means[district][col]
else:
modified_data_frame.at[index, col] = 0
print("TRANSFORMATION DONE")
return modified_data_frame
except Exception as e:
print(f"Error in transform_data: {str(e)}")
raise
def load_data_to_snowflake(transformed):
snowflake_params = {
}

conn = connect(**snowflake_params)
cur = conn.cursor()
try:
snowflake_table = 'domesticvisitorstable'
columns = transformed.columns.tolist()
drop_table_statement = f"DROP TABLE IF EXISTS {snowflake_table};"
cur.execute(drop_table_statement)
create_table_statement = f"CREATE TABLE {snowflake_table} ("
for col in columns:
if col in ['District', 'Month']:
create_table_statement += f"{col} VARCHAR(255),"
else:
create_table_statement += f"{col} INT,"
create_table_statement = create_table_statement.rstrip(',')
create_table_statement += ");"
cur.execute(create_table_statement)
for index, row in transformed.iterrows():
values = []
for value in row:
if isinstance(value, str):
values.append(f"'{value}'")
else:
values.append(str(value))
column_names = ', '.join(columns)
row_values = ', '.join(values)
sql_statement = f"INSERT INTO {snowflake_table} ({column_names}) VALUES ({row_values})"
print(f"SQL statement: {sql_statement}")
cur.execute(sql_statement)
conn.commit()
print("LOADING DONE")
except Exception as e:
print(f"Error in load_data_to_snowflake: {str(e)}")
conn.rollback()
finally:
cur.close()
conn.close()
extract_transform_load = PythonOperator(
task_id='extract_transform_load',
28
python_callable=etl_function,
dag=dag,
)
start = DummyOperator(task_id='start')
end = DummyOperator(task_id='end')
start >> extract_transform_load >> end

# Etl_Foreign_Visitors:

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from snowflake.connector import connect
from datetime import datetime
import pandas as pd
import io
dag_id = 'etl_cloud_to_snowflake_foreign_visitors'
schedule_interval = None
default_args = {
'owner': 'Admin',
'start_date': datetime(2024, 1, 29),
'retries': 1,
}
dag = DAG(
dag_id=dag_id,
default_args=default_args,
schedule_interval=schedule_interval,
catchup=False,
max_active_runs=1,
)
s3_bucket_name = 'abc'
aws_access_key = 'xyz'
aws_secret_key = 'xyz'
aws_conn_id='amazon_conn'
def etl_function():
print("EXTRACTION STARTED")
extraction_returned_data = extract_data_from_s3(s3_bucket_name, aws_conn_id)
print("TRANSFORMATION STARTED")
transformation_returned_data = transform_data(extraction_returned_data)
print("LOADING STARTED")
29
load_data_to_snowflake(transformation_returned_data)
print("ALL DONE")
def extract_data_from_s3(bucket_name, aws_conn_id):
try:
s3_hook = S3Hook(aws_conn_id)
s3_objects = s3_hook.list_keys(bucket_name=bucket_name)
if not s3_objects:
raise Exception(f"No objects found in the S3 bucket '{bucket_name}'.")
latest_object = max(s3_objects)
file_content = s3_hook.read_key(latest_object, bucket_name)
data_frame = pd.read_csv(io.StringIO(file_content))
print("EXTRACTION DONE")
return data_frame
except Exception as e:
print(f"Error in extract_data_from_s3: {str(e)}")
raise
def transform_data(extracted):
try:
data_frame = extracted.copy()
all_columns = data_frame.columns.tolist()
non_visitor_columns = ['District','Month']
visitor_columns=[]
for i in all_columns:
if i not in non_visitor_columns:
visitor_columns.append(i)
for col in visitor_columns:
new_col_values = []
for value in data_frame[col]:
if pd.isna(value) or not value.strip().isdigit():
new_col_values.append(0)
else:
new_col_values.append(int(value))
data_frame[col] = new_col_values
district_sums = {}
district_counts = {}
for index, row in data_frame.iterrows():
district = row['District']
for col in visitor_columns:
if row[col] != 0:
key = (district, col)
30
if key not in district_sums:
district_sums[key] = 0
district_counts[key] = 0
district_sums[key] += row[col]
district_counts[key] += 1
district_means = {}
for (district, col), value_sum in district_sums.items():
count = district_counts[(district, col)]
if district not in district_means:
district_means[district] = {}
if count > 0:
district_means[district][col] = value_sum // count
else:
district_means[district][col] = 0
modified_data_frame = data_frame.copy()
for index, row in data_frame.iterrows():
district = row['District']
for col in visitor_columns:
if row[col] == 0:
if district in district_means and col in district_means[district]:
modified_data_frame.at[index, col] = district_means[district][col]
else:
modified_data_frame.at[index, col] = 0
print("TRANSFORMATION DONE")
return modified_data_frame
except Exception as e:
print(f"Error in transform_data: {str(e)}")
raise
def load_data_to_snowflake(transformed):
snowflake_params = {
}
conn = connect(**snowflake_params)
cur = conn.cursor()
try:

snowflake_table = 'foreignvisitorstable'
columns = transformed.columns.tolist()
drop_table_statement = f"DROP TABLE IF EXISTS {snowflake_table};"
cur.execute(drop_table_statement)
create_table_statement = f"CREATE TABLE {snowflake_table} ("
for col in columns:
if col in ['District', 'Month']:
create_table_statement += f"{col} VARCHAR(255),"
else:
create_table_statement += f"{col} INT,"
create_table_statement = create_table_statement.rstrip(',')
create_table_statement += ");"
cur.execute(create_table_statement)
for index, row in transformed.iterrows():
values = []
for value in row:
if isinstance(value, str):
values.append(f"'{value}'")
else:
values.append(str(value))
column_names = ', '.join(columns)
row_values = ', '.join(values)
sql_statement = f"INSERT INTO {snowflake_table} ({column_names}) VALUES ({row_values})"
print(f"SQL statement: {sql_statement}")
cur.execute(sql_statement)
conn.commit()
print("LOADING DONE")
except Exception as e:
print(f"Error in load_data_to_snowflake: {str(e)}")
conn.rollback()
finally:
cur.close()
conn.close()
extract_transform_load = PythonOperator(
task_id='extract_transform_load',
python_callable=etl_function,
dag=dag,
)

start = DummyOperator(task_id='start')
end = DummyOperator(task_id='end')
start >> extract_transform_load >> end
