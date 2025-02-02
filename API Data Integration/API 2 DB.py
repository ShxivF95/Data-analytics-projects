import requests
import psycopg2
from datetime import datetime


url = "https://dynamics.suraksha.care:8265/patient?start=1&size=241474"
response = requests.get(url)
data = response.json()
data = data["list"]

if response.status_code == 200:    
    print(f"{len(data)} records fetched successfully!")
else:
    print(f"Error: {response.status_code} - {response.text}")
    


try:
    conn = psycopg2.connect(
        dbname="sdpl",
        user="postgres",
        password="8810",
        host="localhost",
        port=5432
    )
    print("Connected to the database successfully!")
except psycopg2.OperationalError as oe:
    print(f"Database connection failed: {oe}")
    

cursor = conn.cursor()


create_table = """
CREATE TABLE IF NOT EXISTS Patient_Details(
    title TEXT,
    patientID VARCHAR(255),
    firstName TEXT,
    middleName TEXT,
    lastName TEXT,
    gender TEXT,
    ageYear INT,
    ageMonth INT,
    ageDay INT,
    dateOfBirth DATE,
    mobile VARCHAR(255),
    email VARCHAR(255),
    address VARCHAR(255),
    city TEXT,
    state TEXT,
    county TEXT,
    pinCode VARCHAR(255),
    employeeNo VARCHAR(255),
    phone VARCHAR(255) NULL,
    custAccount VARCHAR(255)
);"""

cursor.execute(create_table)
conn.commit()


rows_to_insert = []
for item in data:
    dob = item.get("dateOfBirth")
    formatted_dob = datetime.strptime(dob, "%d/%m/%Y").strftime("%Y-%m-%d") 

    rows_to_insert.append(
           (item.get("title"),
            item.get("patientID"),
            item.get("firstName"),
            item.get("middleName"),
            item.get("lastName"),
            item.get("gender"),
            item.get("ageYear"), 
            item.get("ageMonth"),
            item.get("ageDay"), 
            formatted_dob,
            item.get("mobile"),
            item.get("email"),
            item.get("address"),
            item.get("city"),
            item.get("state"),
            item.get("county"),
            item.get("pinCode"),
            item.get("employeeNo"),
            item.get("phone"),
            item.get("custAccount")
    ))
    


insert_query = """
INSERT INTO Patient_Details (
    title, patientID, firstName, middleName, lastName, gender, ageYear, ageMonth,
    ageDay, dateOfBirth, mobile, email, address, city, state, county, pinCode, 
    employeeNo, phone, custAccount
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

cursor.executemany(insert_query, rows_to_insert)
conn.commit()
print(f"{len(rows_to_insert)} rows inserted successfully!")



