from bs4 import BeautifulSoup
import html


def clean_description(description):
    """Clean HTML content from job description"""
    if not isinstance(description, str):
        return ""
    unescaped = html.unescape(description)
    soup = BeautifulSoup(unescaped, "html.parser")
    return soup.get_text(separator="\n").strip()

