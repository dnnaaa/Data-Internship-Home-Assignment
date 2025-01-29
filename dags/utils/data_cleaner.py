from bs4 import BeautifulSoup
import html
import re

def clean_description(description):
    """Clean HTML content from job description"""
    if not isinstance(description, str):
        return ""
    
    # Convert HTML entities to characters first (e.g., &lt; → "<")
    unescaped = html.unescape(description)

    # Parse the HTML content first
    soup = BeautifulSoup(unescaped, "html.parser")
    text = soup.get_text(separator="\n").strip()

    # Unescape HTML entities after parsing
    unescaped = html.unescape(text)

    # Remove extra spaces and newlines
    cleaned_text = re.sub(r'\s+', ' ', unescaped).strip()

    return cleaned_text
