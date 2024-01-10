import os
import json
import re
from bs4 import BeautifulSoup
from typing import Any

def add_cwd_to_file_path(file_path: str) -> str:
    """
    Constructs a full file path by joining the provided file path with the current working directory.

    Args:
        file_path: The file path to be appended to the current working directory.

    Returns:
        str: The full file path combined with the current working directory.
    """
    return os.path.join(
        os.getcwd(),
        file_path
    )

def create_file_dir(file_dir: str) -> None:
    """
    Creates a directory if it doesn't exist.

    Args:
        file_dir: The directory path to be created.

    Returns:
        None: This function does not return anything. It creates a directory at the specified path.
    """
    if not os.path.isdir(file_dir):
        os.makedirs(file_dir)

def write_txt(file_path: str, txt_content: str, encoding: str = "utf-8") -> None:
    """
    Writes text content to a file at the specified path.

    Args:
        file_path: The path where the text file will be created.
        txt_content: The text content to write to the file.
        encoding: The encoding to use for the file. Default is 'utf-8'.

    Returns:
        None: This function does not return anything. It writes the text content to the specified file.
    """
    with open(file=file_path, mode="w", encoding=encoding) as f:
        f.write(txt_content)

def natural_sort_key(s: str) -> list:
    """
    Generates a natural sorting key for strings (e.g., sorting '2' before '10').

    Args:
        s: The string to be sorted.

    Returns:
        list: A list of strings and integers extracted from the input string for sorting purposes.
    """
    return [int(text) if text.isdigit() else text.lower() for text in re.split('([0-9]+)', s)]

def read_json(file_path: str, encoding: str = "utf-8") -> Any:
    """
    Reads a text file containing JSON and returns its content

    Args:
        file_path: The path to the text file.
        encoding: The encoding of the file. Default is 'utf-8'.

    Returns:
        Any: The content of the JSON file.
    """
    with open(file=file_path, mode="r", encoding=encoding) as f:
        # Use json.load to load the content of the JSON file
        return json.load(f)

def write_json(file_path: str, json_content: dict, encoding: str = "utf-8") -> None:
    """
    Writes a dictionary as a JSON file at the specified path.

    Args:
        file_path: The path where the JSON file will be created.
        json_content: The dictionary content to write to the file as JSON.
        encoding: The encoding to use for the file. Default is 'utf-8'.

    Returns:
        None: This function does not return anything. It writes the dictionary as a JSON file.
    """
    with open(file=file_path, mode="w", encoding=encoding) as f:
        json.dump(json_content, f, indent=2)  # indent=2 for pretty formatting

def list_files_sorted(files_dir: str) -> list:
    """
    Lists files in natural alphabetical and numerical order.

    Args:
        files_dir: the drirectory to the files.

    Returns:
        list: the list of the ordered files.
    """
    return sorted(
        os.listdir(files_dir),
        key=natural_sort_key
    )