import requests
import json
import datetime
import time
from utils import console, load_json_file, set_dict_entry, move_by_dates, save_output, move_by_dates_101

args = {
    "path": "/Cargas de cámara",
    "to_path": "/Photos/folder/iPhone",
    "from_date": "20180624", # Format YYYYMMDD
    "to_date": "20180812", # Format YYYYMMDD
    "recursive": True,
    "limit": 2000,
    "wait_time": 90 # Only required by move_by_dates function, move_by_dates_101 doesn't need it
}

def main():
    #move_by_dates(args)
    move_by_dates_101(args)
    save_output(console, "log/console.json")

if __name__ == '__main__':
    main()