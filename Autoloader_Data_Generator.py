# Databricks notebook source
import csv
import uuid
import random
import time
from pathlib import Path

count = 0
path = "/tmp/generated_raw_csv_data"
Path(path).mkdir(parents=True, exist_ok=True)

while True:
    row_list = [ ["id", "x_axis", "y_axis"],
               [uuid.uuid4(), random.randint(-100, 100), random.randint(-100, 100)],
               [uuid.uuid4(), random.randint(-100, 100), random.randint(-100, 100)],
               [uuid.uuid4(), random.randint(-100, 100), random.randint(-100, 100)]
             ]
    file_location = f'{path}/file_{count}.csv'

    with open(file_location, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(row_list)
        file.close()

    count += 1
    dbutils.fs.mv(f'file:{file_location}', f'dbfs:{file_location}')
    time.sleep(30)
    print(f'New CSV file created at dbfs:{file_location}. Contents:')

    with open(f'/dbfs{file_location}', 'r') as file:
        reader = csv.reader(file, delimiter=' ')
        for row in reader:
            print(', '.join(row))
        file.close()
    break

# COMMAND ----------

import csv
import uuid
import random
import time
from pathlib import Path

count = 1
path = "/tmp/generated_raw_csv_data"
Path(path).mkdir(parents=True, exist_ok=True)

while True:
    row_list = [ ["id", "x_axis", "y_axis"],
               [uuid.uuid4(), f'{random.randint(-100, 100)}z', random.randint(-100, 100)],
               [uuid.uuid4(), random.randint(-100, 100), random.randint(-100, 100)],
               [uuid.uuid4(), random.randint(-100, 100), random.randint(-100, 100)]
             ]
    file_location = f'{path}/file_{count}.csv'

    with open(file_location, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(row_list)
        file.close()

    count += 1
    dbutils.fs.mv(f'file:{file_location}', f'dbfs:{file_location}')
    time.sleep(30)
    print(f'New CSV file created at dbfs:{file_location}. Contents:')

    with open(f'/dbfs{file_location}', 'r') as file:
        reader = csv.reader(file, delimiter=' ')
        for row in reader:
            print(', '.join(row))
        file.close()
    break

# COMMAND ----------

dbutils.fs.ls('/tmp/table/')

# COMMAND ----------

dbutils.fs.rm("dbfs:/tmp/generated_raw_csv_data", True)
dbutils.fs.rm("dbfs:/tmp/auto_loader/schema",True)
dbutils.fs.rm("dbfs:/tmp/table/coordinates",True)
dbutils.fs.rm("dbfs:/tmp/auto_loader/checkpoint",True)

# COMMAND ----------

import csv
import uuid
import random
import time
from pathlib import Path

count = 3
path = "/tmp/generated_raw_csv_data"
Path(path).mkdir(parents=True, exist_ok=True)

while True:
    row_list = [ ["id", "x_axis", "y_axis","z_axis"],
               [uuid.uuid4(), random.randint(-100, 100), random.randint(-100, 100),random.randint(-100, 100)],
               [uuid.uuid4(), random.randint(-100, 100), random.randint(-100, 100),random.randint(-100, 100)],
               [uuid.uuid4(), random.randint(-100, 100), random.randint(-100, 100),random.randint(-100, 100)]
             ]
    file_location = f'{path}/file_{count}.csv'

    with open(file_location, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(row_list)
        file.close()

    count += 1
    dbutils.fs.mv(f'file:{file_location}', f'dbfs:{file_location}')
    time.sleep(30)
    print(f'New CSV file created at dbfs:{file_location}. Contents:')

    with open(f'/dbfs{file_location}', 'r') as file:
        reader = csv.reader(file, delimiter=' ')
        for row in reader:
            print(', '.join(row))
        file.close()
    break

# COMMAND ----------


