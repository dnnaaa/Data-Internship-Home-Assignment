# from bs4 import BeautifulSoup
# import html
# import re


# def clean_description(description):
#     """Clean HTML content from job description"""
#     if not isinstance(description, str):
#         return ""
#     unescaped = html.unescape(description)
#     soup = BeautifulSoup(unescaped, "html.parser")
#     text = soup.get_text(separator="\n").strip()

#     # Remove extra spaces and newlines
#     cleaned_text = re.sub(r'\s+', ' ', text).strip()

#     return cleaned_text

from bs4 import BeautifulSoup
import html
import re

def clean_description(description):
    """Clean HTML content from job description"""
    if not isinstance(description, str):
        return ""
    
    # Parse the HTML content first
    soup = BeautifulSoup(description, "html.parser")
    text = soup.get_text(separator="\n").strip()

    # Unescape HTML entities after parsing
    unescaped = html.unescape(text)

    # Remove extra spaces and newlines
    cleaned_text = re.sub(r'\s+', ' ', unescaped).strip()

    return cleaned_text
