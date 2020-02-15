import requests
import json
import datetime
import time
from utils import console, load_json_file, set_dict_entry, move_by_dates, save_output

args = {
    "path": "/Cargas de cámara",
    "to_path": "/Cargas de cámara/2011",
    "from_date": "20110101", # Format YYYYMMDD
    "to_date": "20111231", # Format YYYYMMDD
    "recursive": True,
    "limit": 2000,
    "wait_time": 90
}

def main():
    move_by_dates(args)
    save_output(console, "log/console.json")

if __name__ == '__main__':
    main()