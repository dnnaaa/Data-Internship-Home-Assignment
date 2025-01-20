import json
import os

class JobsTransformer:
    def __init__(self, input_path="staging/extracted", output_path="staging/transformed"):
        self.input_path = input_path
        self.output_path = output_path
        
    def transform(self):
        """Transform extracted data according to the desired schema."""
        try:
            os.makedirs(self.output_path, exist_ok=True)
            transformed_count = 0
            
            for filename in os.listdir(self.input_path):
                if filename.startswith('job_'):
                    input_file = os.path.join(self.input_path, filename)
                    output_file = os.path.join(self.output_path, 
                                             f"{os.path.splitext(filename)[0]}.json")
                    
                    # Read and parse JSON data
                    with open(input_file, 'r', encoding='utf-8') as f:
                        data = json.loads(f.read())
                    
                    # Transform data
                    transformed_data = self._transform_job_data(data)
                    
                    # Save transformed data
                    with open(output_file, 'w', encoding='utf-8') as f:
                        json.dump(transformed_data, f, indent=2)
                    
                    transformed_count += 1
            
            return transformed_count
            
        except Exception as e:
            raise Exception(f"Error in transformation: {str(e)}")
    
    def _transform_job_data(self, data):
        """Transform job data into the required schema."""
        return {
            "job": {
                "title": data.get("title", ""),
                "industry": data.get("industry", ""),
                "description": data.get("description", ""),
                "employment_type": data.get("employmentType", ""),
                "date_posted": data.get("datePosted", ""),
            },
            "company": {
                "name": data.get("hiringOrganization", {}).get("name", ""),
                "link": data.get("hiringOrganization", {}).get("sameAs", ""),
            },
            "education": {
                "required_credential": data.get("educationRequirements", {}).get("credentialCategory", ""),
            },
            "experience": {
                "months_of_experience": self._extract_experience_months(data),
                "seniority_level": data.get("experienceRequirements", {}).get("seniority", ""),
            },
            "salary": {
                "currency": data.get("baseSalary", {}).get("currency", ""),
                "min_value": data.get("baseSalary", {}).get("value", {}).get("minValue", None),
                "max_value": data.get("baseSalary", {}).get("value", {}).get("maxValue", None),
                "unit": data.get("baseSalary", {}).get("unit", ""),
            },
            "location": {
                "country": data.get("jobLocation", {}).get("address", {}).get("addressCountry", ""),
                "locality": data.get("jobLocation", {}).get("address", {}).get("addressLocality", ""),
                "region": data.get("jobLocation", {}).get("address", {}).get("addressRegion", ""),
                "postal_code": data.get("jobLocation", {}).get("address", {}).get("postalCode", ""),
                "street_address": data.get("jobLocation", {}).get("address", {}).get("streetAddress", ""),
                "latitude": data.get("jobLocation", {}).get("geo", {}).get("latitude", None),
                "longitude": data.get("jobLocation", {}).get("geo", {}).get("longitude", None),
            }
        }
    
    def _extract_experience_months(self, data):
        """Extract and convert experience requirement to months."""
        try:
            exp = data.get("experienceRequirements", {}).get("monthsOfExperience", 0)
            return int(exp) if exp is not None else 0
        except (ValueError, TypeError):
            return 0