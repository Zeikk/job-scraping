import os

from dotenv import  load_dotenv

load_dotenv()

config = {
    "DB_USER": os.environ['DB_USER'],
    "DB_PWD": os.environ['DB_PWD'],
}