import pytest
from bs4 import BeautifulSoup
import html
import re

from dags.utils.data_cleaner import clean_description 

def test_basic_html_cleaning():
    input_text = "<p>This is a <b>test</b> description.</p>"
    expected_output = "This is a test description."
    assert clean_description(input_text) == expected_output

def test_html_entities():
    input_text = "This is a &lt;test&gt; description."
    expected_output = "This is a <test> description."
    assert clean_description(input_text) == expected_output

def test_extra_whitespace():
    input_text = "  This   is   a   test   description.  "
    expected_output = "This is a test description."
    assert clean_description(input_text) == expected_output

def test_mixed_html_and_text():
    input_text = "<div><p>This is a <b>test</b> description 10.</p></div>"
    expected_output = "This is a test description 10."
    assert clean_description(input_text) == expected_output

def test_empty_input():
    input_text = ""
    expected_output = ""
    assert clean_description(input_text) == expected_output

def test_non_string_input():
    input_text = 12345
    expected_output = ""
    assert clean_description(input_text) == expected_output

def test_multiple_newlines_and_spaces():
    input_text = "This is a\ntest\n\ndescription.\n\n"
    expected_output = "This is a test description."
    assert clean_description(input_text) == expected_output