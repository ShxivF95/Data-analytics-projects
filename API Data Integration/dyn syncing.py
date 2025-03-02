import requests
import xml.etree.ElementTree as ET
import psycopg2
from datetime import datetime, timedelta


# url = "http://10.200.0.9/SDPL/AxAPIs/Ax_ReportingDr.aspx?SecurityCode=!Ax_ReportingDr&FromDate= 2023-04-01&ToDate=2025-02-28&FromTime=00:00:00&ToTime=23:59:59"

#  Set dynamic date range (Yesterday -> Today)

yesterday = (datetime.now() - timedelta(days= 1)).strftime('%Y-%m-%d')
today = datetime.now().strftime('%Y-%m-%d')


# using f-string, Python will replace {yesterday} and {today} with the actual values.

url = f"http://10.200.0.9/SDPL/AxAPIs/Ax_ReportingDr.aspx?SecurityCode=!Ax_ReportingDr&FromDate= {yesterday}&ToDate= {today}&FromTime=00:00:00&ToTime=23:59:59"    


response = requests.get(url)
xml_content = response.text.strip()

root = ET.fromstring(xml_content)

data = []
for child in root:
    record = {subchild.tag: subchild.text  for subchild in child}
    data.append(record)

conn = psycopg2.connect(
    dbname= "sdpl",
    user= "postgres",
    password= "4321",
    host= "localhost",
    port= "5432"
)

cursor = conn.cursor()

create_table = """
CREATE TABLE IF NOT EXISTS
ACX_ReportingDr(
    Test_ID VARCHAR(20),            
    Investigation_ID VARCHAR(20),   
    LedgerTransactionNo VARCHAR(20),
    ApprovalDateTime TIMESTAMP,     
    DoctorID VARCHAR(20),           
    DoctorName VARCHAR(255),        
    ResultDateTime TIMESTAMP,      
    TechnicianID VARCHAR(20),       
    TechnicianName VARCHAR(255),
    UNIQUE (Test_ID, Investigation_ID)
    )"""

cursor.execute(create_table)
conn.commit()

rows_to_insert = [(item.get("Test_ID"), item.get("Investigation_ID"), item.get("LedgerTransactionNo"), item.get("ApprovalDateTime"),
                   item.get("DoctorID"), item.get("DoctorName"), item.get("ResultDateTime"), item.get("TechnicianID"), item.get("TechnicianName")) for item in data]

insert_query = """
INSERT INTO ACX_ReportingDr(Test_ID, Investigation_ID, LedgerTransactionNo, ApprovalDateTime, DoctorID, DoctorName, ResultDateTime, TechnicianID, TechnicianName)
VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (Test_ID, Investigation_ID)
DO NOTHING"""

cursor.executemany(insert_query, rows_to_insert)
conn.commit()

print(f"{len(rows_to_insert)} rows inserted successfully")

