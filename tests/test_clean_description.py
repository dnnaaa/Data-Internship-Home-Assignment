from dags.etl import clean_description

def test_clean_description():
    input_description = "<p>This is a <b>test</b> description.</p>"
    expected_output = "this is a test description"

    cleaned_description = clean_description(input_description)

    assert cleaned_description == expected_output