import requests
import psycopg2

url= "https://dynamics.suraksha.care:8265/baseProductPrice"
response= requests.get(url)
data = response.json()

if response.status_code == 200:
    print(data)
else:
    print(f"Error: {response.status_code} - {response.text}")

try:
    conn= psycopg2.connect(
        dbname= "sdpl",
        user= "postgres",
        password= "8810",
        host= "localhost",
        port= 5432
    )
    print("Connected to the database successfuly")
except psycopg2.OperationalError as oe:
    print("Database connection failed: {oe}")


cursor= conn.cursor()

create_table= """
CREATE TABLE IF NOT EXISTS production_price(
     
     itemid varchar(255),
     configid varchar(255),
     price decimal(10,2)
    );
    """

cursor.execute(create_table)
conn.commit()

rows_to_insert = [(item.get("itemid"), item.get("configid"), item.get("price")) for item in data]

insert_query= """
INSERT INTO production_price(itemid, configid, price)
VALUES(%s, %s, %s)
"""

cursor.executemany(insert_query, rows_to_insert)
conn.commit()

print(f"{len(rows_to_insert)} rows inserted successfully.")






