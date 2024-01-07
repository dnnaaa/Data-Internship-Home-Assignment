def test_transform():
    # Assuming your actual transform function modifies the data structure
    data = pd.DataFrame({
    "job_title": ["Title1", "Title2"],
    "job_industry": ["Industry1", "Industry2"],
    "description": ["<b>Description1</b>", "<i>Description2</i>"],
    "employmentType": ["Full-time", "Part-time"],  # Add the actual values
    "datePosted": ["2022-01-01", "2022-01-02"],  # Add the actual values
    "hiringOrganization.name": ["Company1", "Company2"],  # Add the actual values
    "hiringOrganization.sameAs": ["https://linkedin.com/company1", "https://linkedin.com/company2"],  # Add the actual values
    "educationRequirements.credentialCategory": ["Degree1", "Degree2"],  # Add the actual values
    "experienceRequirements.monthsOfExperience": [24, 36],  # Add the actual values
    "experienceRequirements.seniorityLevel": ["Mid-level", "Senior"],  # Add the actual values
    "salary.currency": ["USD", "EUR"],  # Add the actual values
    "salary.min_value": [50000, 60000],  # Add the actual values
    "salary.max_value": [80000, 90000],  # Add the actual values
    "salary.unit": ["Year", "Year"],  # Add the actual values
    "jobLocation.address.addressCountry": ["Country1", "Country2"],  # Add the actual values
    "jobLocation.address.addressLocality": ["Locality1", "Locality2"],  # Add the actual values
    "jobLocation.address.addressRegion": ["Region1", "Region2"],  # Add the actual values
    "jobLocation.address.postalCode": ["12345", "67890"],  # Add the actual values
    "jobLocation.address.streetAddress": ["Street1", "Street2"],  # Add the actual values
    "jobLocation.latitude": [12.345, 23.456],  # Add the actual values
    "jobLocation.longitude": [45.678, 56.789],  # Add the actual values
 
})


    transformed_data = transform(data)

    # Check the transformed data structure for the first item
    expected_transformed_item = {
        "job": {
            "title": "Title1",
            "industry": "Industry1",
            "description": "Description1",  # Cleaned description
            "employment_type": None,  # Replace None with the actual value
            "date_posted": None,  # Replace None with the actual value
        },
        "company": {
            "name": None,  # Replace None with the actual value
            "link": None,  # Replace None with the actual value
        },
        "education": {
            "required_credential": None,  # Replace None with the actual value
        },
        "experience": {
            "months_of_experience": None,  # Replace None with the actual value
            "seniority_level": None,  # Replace None with the actual value
        },
        "salary": {
            "currency": None,  # Replace None with the actual value
            "min_value": None,  # Replace None with the actual value
            "max_value": None,  # Replace None with the actual value
            "unit": None,  # Replace None with the actual value
        },
        "location": {
            "country": None,  # Replace None with the actual value
            "locality": None,  # Replace None with the actual value
            "region": None,  # Replace None with the actual value
            "postal_code": None,  # Replace None with the actual value
            "street_address": None,  # Replace None with the actual value
            "latitude": None,  # Replace None with the actual value
            "longitude": None,  # Replace None with the actual value
        },
    }

    assert transformed_data[0] == expected_transformed_item

        # Check the transformed data structure for the second item
    expected_transformed_item_2 = {
        "job": {
            "title": "Title2",
            "industry": "Industry2",
            "description": "Description2",  # Cleaned description
            "employment_type": "Part-time",  # Add the actual value
            "date_posted": "2022-01-02",  # Add the actual value
        },
        "company": {
            "name": "Company2",  # Add the actual value
            "link": "https://linkedin.com/company2",  # Add the actual value
        },
        "education": {
            "required_credential": "Degree2",  # Add the actual value
        },
        "experience": {
            "months_of_experience": 36,  # Add the actual value
            "seniority_level": "Senior",  # Add the actual value
        },
        "salary": {
            "currency": "EUR",  # Add the actual value
            "min_value": 60000,  # Add the actual value
            "max_value": 90000,  # Add the actual value
            "unit": "Year",  # Add the actual value
        },
        "location": {
            "country": "Country2",  # Add the actual value
            "locality": "Locality2",  # Add the actual value
            "region": "Region2",  # Add the actual value
            "postal_code": "67890",  # Add the actual value
            "street_address": "Street2",  # Add the actual value
            "latitude": 23.456,  # Add the actual value
            "longitude": 56.789,  # Add the actual value
        },
        "benefits": {
        "healthcare": True,  # Add the actual value
        "vacation_days": 20,  # Add the actual value
        # ... other benefit-related fields
    },
        "skills": {
            "required": ["Python", "SQL"],  # Add the actual value
            "preferred": ["Machine Learning", "Data Visualization"],  # Add the actual value
    }

    assert transformed_data[1] == expected_transformed_item_2

# Add more test cases as needed
