from bs4 import BeautifulSoup
import html
import re


def clean_description(description):
    """Clean HTML content from job description"""
    if not isinstance(description, str):
        return ""
    unescaped = html.unescape(description)
    soup = BeautifulSoup(unescaped, "html.parser")
    text = soup.get_text(separator="\n").strip()

    # Remove extra spaces and newlines
    cleaned_text = re.sub(r'\s+', ' ', text).strip()

    return cleaned_text