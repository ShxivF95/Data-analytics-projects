import requests
import psycopg2

url = 'https://dynamics.suraksha.care:8265/baseProductPrice'
response = requests.get(url)
data = response.json()

if response.status_code == 200:
    print(f"{len(data)} rows are there")
else:
    print(f"Error: {response.status_code} - {response.text}")


try:
    conn = psycopg2.connect(
        dbname= "sdpl",
        user= "postgres",
        password= "4321",
        host= "localhost",
        port= "5432"
    )
    print("Connected to the database successfully")
except psycopg2.OperationalError as oe:
    print("Database connection failed: {oe}")


cursor = conn.cursor()

create_table = """
    CREATE TABLE IF NOT EXISTS product_price (
        itemid VARCHAR(255),
        configid VARCHAR(255),
        price DECIMAL(10,2),
        PRIMARY KEY (itemid, configid)
    );
    """

cursor.execute(create_table)
conn.commit()


rows_to_insert = [(item.get("itemid"), item.get("configid"), item.get("price")) for item in data]

insert_query = """
INSERT INTO product_price (itemid, configid, price)
VALUES (%s, %s, %s)
ON CONFLICT (itemid, configid) 
DO UPDATE 
SET price = EXCLUDED.price;;"""

cursor.executemany(insert_query, rows_to_insert)
conn.commit()
print(f"{len(rows_to_insert)} rows inserted successfully.")