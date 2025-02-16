import requests
import psycopg2
from datetime import datetime

url = 'https://dynamics.suraksha.care:8265/patient?start=1&size=241474'
response = requests.get(url)
data = response.json()
data = data['list']

if response.status_code == 200:
    print(f"{len(data)} rows are there.")
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
    print("Connected to the database successfully.")
except psycopg2.OperationalError as oe:
    print(f"Database connection failed: {oe}")

cursor = conn.cursor()

create_table = """
CREATE TABLE IF NOT EXISTS
patient_details(
title VARCHAR(10),
patientID VARCHAR(20) PRIMARY KEY,
firstName VARCHAR(50),
middleName VARCHAR(50),
lastName VARCHAR(50),
gender VARCHAR(10),
ageYear	INTEGER,
ageMonth INTEGER,
ageDay INTEGER,
dateOfBirth DATE,
mobile VARCHAR(20),
email VARCHAR(100),
address TEXT,
city VARCHAR(50),
state VARCHAR(50),
county VARCHAR(50),
pinCode VARCHAR(10),
employeeNo	VARCHAR(20),
phone VARCHAR(20),
custAccount	VARCHAR(20)
);"""

cursor.execute(create_table)
conn.commit()
print("Table created successfully")

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
        item.get("custAccount"))
    )

insert_query = """
INSERT INTO patient_details(title, patientID, firstName, middleName, lastName, gender, ageYear, ageMonth, ageDay, dateOfBirth, mobile,
                            email, address, city, state, county, pinCode, employeeNo, phone, custAccount)
VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT(patientID)
DO UPDATE
SET title = EXCLUDED.title,
    firstName = EXCLUDED.firstName,
    middleName = EXCLUDED.middleName,
    lastName = EXCLUDED.lastName,
    gender = EXCLUDED.gender,
    ageYear = EXCLUDED.ageYear,
    ageMonth = EXCLUDED.ageMonth,
    ageDay = EXCLUDED.ageDay,
    dateOfBirth = EXCLUDED.dateOfBirth,
    mobile = EXCLUDED.mobile,
    email = EXCLUDED.email,
    city = EXCLUDED.city,
    state = EXCLUDED.state,
    county = EXCLUDED.county,
    pinCode = EXCLUDED.pinCode,
    employeeNo = EXCLUDED.employeeNo,
    phone = EXCLUDED.phone,
    custAccount = EXCLUDED.custAccount
"""

cursor.executemany(insert_query, rows_to_insert)
conn.commit()
print(f"{len(rows_to_insert)} rows inserted successfully.")






