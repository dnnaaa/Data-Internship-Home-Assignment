import os
import json
import pytest
from dags.transform import transform

def test_transform(tmpdir):
    extracted_dir = tmpdir.mkdir('extracted')
    extracted_dir.join('0.txt').write('{"title": "REMOTE Senior Machine Learning Engineer - ML, AI, Data Science", "industry": "Information Technology and Services,Computer Software,Financial Services"}')
    extracted_dir.join('1.txt').write('{"title": "Platform Solution Engineer, MuleSoft Enterprise", "industry": "Computer Software,Information Technology and Services,Internet"}')

    transform()

    transformed_dir = os.path.join('staging/transformed')
    assert os.path.isdir(transformed_dir)
    assert os.path.isfile(os.path.join(transformed_dir, '0.json'))
    assert os.path.isfile(os.path.join(transformed_dir, '1.json'))

    with open(os.path.join(transformed_dir, '0.json'), 'r') as f:
        data_0 = json.load(f)
        assert data_0['job']['title'] == 'REMOTE Senior Machine Learning Engineer - ML, AI, Data Science'
        assert data_0['job']['industry'] == 'Information Technology and Services,Computer Software,Financial Services'
        

    with open(os.path.join(transformed_dir, '1.json'), 'r') as f:
        data_1 = json.load(f)
        assert data_1['job']['title'] == 'Platform Solution Engineer, MuleSoft Enterprise'
        assert data_1['job']['industry'] == 'Computer Software,Information Technology and Services,Internet'
       

    


    