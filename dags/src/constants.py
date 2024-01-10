from dotenv import load_dotenv
from os import getenv
import src.io_utils as io_utils

# envrionment variables
load_dotenv()
FILE_PATH = io_utils.add_cwd_to_file_path(getenv("FILE_PATH"))

OUTPUT_DIR_EXTRACTED = io_utils.add_cwd_to_file_path(getenv("OUTPUT_DIR_EXTRACTED"))
OUTPUT_DIR_TRANSFORMED = io_utils.add_cwd_to_file_path(getenv("OUTPUT_DIR_TRANSFORMED"))
DB_PATH = getenv("DB_PATH")
