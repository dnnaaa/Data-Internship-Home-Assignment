import sqlite3
import json
import os

def load_data_into_db():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    transformed_dir = os.path.join(script_dir, "staging/transformed")
    db_path = os.path.join(script_dir, "my_db.db")

    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    for filename in os.listdir(transformed_dir):
        if filename.endswith('.json'):
            file_path = os.path.join(transformed_dir, filename)
            
            with open(file_path, 'r') as file:
                data = json.load(file)


                if 'job' in data:
                    c.execute("INSERT INTO job (title, industry, description, employment_type, date_posted) VALUES (?, ?, ?, ?, ?)", 
                              (data['job']['title'], data['job']['industry'], data['job']['description'], data['job']['employment_type'], data['job']['date_posted']))


                if 'company' in data:
                    c.execute("INSERT INTO company (name, link) VALUES (?, ?)", 
                              (data['company']['name'], data['company']['link']))


                if 'education' in data:
                    c.execute("INSERT INTO education (required_credential) VALUES (?)",
                              (data['education']['required_credential'],))


                if 'experience' in data:
                    c.execute("INSERT INTO experience (months_of_experience, seniority_level) VALUES (?, ?)",
                              (data['experience']['months_of_experience'], data['experience']['seniority_level']))


                if 'salary' in data:
                    c.execute("INSERT INTO salary (currency, min_value, max_value, unit) VALUES (?, ?, ?, ?)",
                              (data['salary']['currency'], data['salary']['min_value'], data['salary']['max_value'], data['salary']['unit']))

                if 'location' in data:
                    c.execute("INSERT INTO location (country, locality, region, postal_code, street_address, latitude, longitude) VALUES (?, ?, ?, ?, ?, ?, ?)",
                              (data['location']['country'], data['location']['locality'], data['location']['region'], data['location']['postal_code'], data['location']['street_address'], data['location']['latitude'], data['location']['longitude']))


    conn.commit()
    conn.close()
