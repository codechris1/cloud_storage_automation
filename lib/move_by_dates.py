import requests
import json
import datetime
import time
from utils import console, load_json_file, set_dict_entry, move_by_dates, save_output, move_by_dates_101

args = {
    "path": "/Cargas de cámara/2017",
    "to_path": "/Photos/your folder/iPhone",
    "from_date": "20170508", # Format YYYYMMDD
    "to_date": "20170512", # Format YYYYMMDD
    "recursive": False,
    "limit": 2000,
    "wait_time": 90, # Only required by move_by_dates function, move_by_dates_101 doesn't need it
    "move_criteria": 1  # 1 = by date , the previous criteria will be used | 2 by origin folder (it will move all files from origin folder)
}

def main():
    #move_by_dates(args)
    move_by_dates_101(args)
    save_output(console, "log/console.json")

if __name__ == '__main__':
    main()