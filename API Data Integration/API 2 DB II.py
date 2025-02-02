import requests
import psycopg2

url= "https://dynamics.suraksha.care:8265/allCustomer"
response= requests.get(url)
data= response.json()

if response.status_code == 200:
    print(data)
else:
    print(f"Error: {response.status_code} - {response.text}")

try:
    conn = psycopg2.connect(
        dbname= "sdpl",
        user= "postgres",
        password= "8810",
        host= "localhost",
        port= 5432
    )
    print("Connected to the database successfully.")
except psycopg2.OperationalError as oe:
    print("Database connection failed: {oe}")

cursor= conn.cursor()

create_table= """
CREATE TABLE IF NOT EXISTS custtable(
    custname TEXT,
    accountnum VARCHAR(255),
    pricegroup TEXT,
    acX_CUSTOMERTYPE TEXT,
    blocked TEXT,
    paymmode TEXT,
    paymtermid TEXT,
    createdby VARCHAR(255),
    createddatetime TIMESTAMP,
    modifiedby VARCHAR(255),
    modifieddatetime TIMESTAMP,
    linedisc TEXT,
    acX_ORGANIZATIONTYPE TEXT,
    workername TEXT NULL,   -- Can be NULL if missing
    custmobile VARCHAR(255) NULL,  -- Can store phone number as a string
    patientmsg INT NULL     -- Integer with NULL allowed
);"""

cursor.execute(create_table)
conn.commit()
print("Table is created")

rows_to_inserted= [(item.get("custname"), item.get("accountnum"), item.get("pricegroup"), item.get("acX_CUSTOMERTYPE"), 
                   item.get("blocked"), item.get("paymmode"), item.get("paymtermid"), item.get("createdby"), item.get("createddatetime"),  
                   item.get("modifiedby"), item.get("modifieddatetime"), item.get("linedisc"), item.get("acX_ORGANIZATIONTYPE"),  
                   item.get("workername"), item.get("custmobile"), item.get("patientmsg")) for item in data]

insert_query= """
INSERT INTO custtable (custname, accountnum, pricegroup, acX_CUSTOMERTYPE, blocked, paymmode, paymtermid, createdby, createddatetime,
                       modifiedby, modifieddatetime, linedisc, acX_ORGANIZATIONTYPE, workername, custmobile, patientmsg)
VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s)
"""

cursor.executemany(insert_query, rows_to_inserted)
conn.commit()
print(f"{len(rows_to_inserted)} rows inserted successfully.")