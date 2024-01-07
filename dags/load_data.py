from airflow.providers.sqlite.hooks.sqlite import SqliteHook

def load_data(**kwargs):
    transformed_path = './staging/transformed'

    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(task_ids='clean_transform')

    sqlite_hook = SqliteHook(sqlite_conn_id='sqlite_default')

    for data in transformed_data:
        job_data = data['job']
        company_data = data.get('company', {})
        education_data = data.get('education', {})
        experience_data = data.get('experience', {})
        salary_data = data.get('salary', {})
        location_data = data.get('location', {})

        job_id = sqlite_hook.insert_rows(
            table='job',
            rows=[(
                job_data['title'],
                job_data['industry'],
                job_data['description'],
                job_data['employment_type'],
                job_data['date_posted'],
            )]
        )

        sqlite_hook.insert_rows(
            table='company',
            rows=[(
                job_id,
                company_data.get('name', ''),
                company_data.get('link', ''),
            )]
        )

        sqlite_hook.insert_rows(
            table='education',
            rows=[(
                job_id,
                education_data.get('required_credential', ''),
            )]
        )

        sqlite_hook.insert_rows(
            table='experience',
            rows=[(
                job_id,
                experience_data.get('months_of_experience', ''),
                experience_data.get('seniority_level', ''),
            )]
        )

        sqlite_hook.insert_rows(
            table='salary',
            rows=[(
                job_id,
                salary_data.get('currency', ''),
                salary_data.get('min_value', ''),
                salary_data.get('max_value', ''),
                salary_data.get('unit', ''),
            )]
        )

        sqlite_hook.insert_rows(
            table='location',
            rows=[(
                job_id,
                location_data.get('country', ''),
                location_data.get('locality', ''),
                location_data.get('region', ''),
                location_data.get('postal_code', ''),
                location_data.get('street_address', ''),
                location_data.get('latitude', ''),
                location_data.get('longitude', ''),
            )]
        )
