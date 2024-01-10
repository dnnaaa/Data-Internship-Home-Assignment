import sqlite3

db_path = '/home/amini/Downloads/Data-Internship-Home-Assignment/airflow.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

q="select count(*) from location "
cursor.execute(q)

results = cursor.fetchall()

# Display the results
for i,row in enumerate(results):
    if i==5:
        break
    print(row)