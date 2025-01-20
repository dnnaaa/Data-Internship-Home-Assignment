import json
import os
from airflow.providers.sqlite.hooks.sqlite import SqliteHook

class SqliteLoader:
    def __init__(self, input_path="staging/transformed"):
        self.input_path = input_path
        self.hook = SqliteHook(sqlite_conn_id='sqlite_default')
    
    def load(self):
        """Load transformed data into SQLite database."""
        try:
            loaded_count = 0
            
            for filename in os.listdir(self.input_path):
                if filename.endswith('.json'):
                    with open(os.path.join(self.input_path, filename), 'r', 
                            encoding='utf-8') as f:
                        data = json.load(f)
                        self._insert_job_data(data)
                        loaded_count += 1
            
            return loaded_count
            
        except Exception as e:
            raise Exception(f"Error in loading: {str(e)}")
    
    def _insert_job_data(self, data):
        """Insert job data into all related tables."""
        # Insert job and get job_id
        job_id = self._insert_job(data['job'])
        
        # Insert related data
        self._insert_company(job_id, data['company'])
        self._insert_education(job_id, data['education'])
        self._insert_experience(job_id, data['experience'])
        self._insert_salary(job_id, data['salary'])
        self._insert_location(job_id, data['location'])
    
    def _insert_job(self, job_data):
        """Insert job data and return job_id."""
        query = """
        INSERT INTO job (title, industry, description, employment_type, date_posted)
        VALUES (?, ?, ?, ?, ?)
        """
        params = [
            job_data['title'],
            job_data['industry'],
            job_data['description'],
            job_data['employment_type'],
            job_data['date_posted']
        ]
        
        return self.hook.run(query, parameters=params)
    
    def _insert_company(self, job_id, company_data):
        query = """
        INSERT INTO company (job_id, name, link)
        VALUES (?, ?, ?)
        """
        params = [job_id, company_data['name'], company_data['link']]
        self.hook.run(query, parameters=params)
    
    def _insert_education(self, job_id, education_data):
        query = """
        INSERT INTO education (job_id, required_credential)
        VALUES (?, ?)
        """
        params = [job_id, education_data['required_credential']]
        self.hook.run(query, parameters=params)
    
    def _insert_experience(self, job_id, experience_data):
        query = """
        INSERT INTO experience (job_id, months_of_experience, seniority_level)
        VALUES (?, ?, ?)
        """
        params = [
            job_id,
            experience_data['months_of_experience'],
            experience_data['seniority_level']
        ]
        self.hook.run(query, parameters=params)
    
    def _insert_salary(self, job_id, salary_data):
        query = """
        INSERT INTO salary (job_id, currency, min_value, max_value, unit)
        VALUES (?, ?, ?, ?, ?)
        """
        params = [
            job_id,
            salary_data['currency'],
            salary_data['min_value'],
            salary_data['max_value'],
            salary_data['unit']
        ]
        self.hook.run(query, parameters=params)
    
    def _insert_location(self, job_id, location_data):
        query = """
        INSERT INTO location (
            job_id, country, locality, region, postal_code,
            street_address, latitude, longitude
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
        params = [
            job_id,
            location_data['country'],
            location_data['locality'],
            location_data['region'],
            location_data['postal_code'],
            location_data['street_address'],
            location_data['latitude'],
            location_data['longitude']
        ]
        self.hook.run(query, parameters=params)