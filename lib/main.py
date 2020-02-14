import requests
import json
import datetime
import time

args = {
    "api_key": "",
    "path": "/Cargas de cámara",
    "to_path": "/Cargas de cámara/2019",
    "from_date": "20190101", # Format YYYYMMDD
    "to_date": "20191231", # Format YYYYMMDD
    "recursive": True,
    "limit": 2000
}

def main():
    extract(args['api_key'], args['path'], args['recursive'], args['limit'])
    move_files(args)

def move_files(args):
    files = load_json_file("files")
    files_to_move = []
    log = []
    log_human = []
    error = []
    console = []
    total_files_to_move = 0
    for file in files:
        #if range_date(file['name'], args['from_date'], args['to_date']) and (args['to_path'].lower() not in file['path_lower']):
        if range_date(file['name'], args['from_date'], args['to_date']):
            files_to_move.append(file)
            total_files_to_move += 1
    to_print = "The total files created between {0} and {1} to move from {2} to {3} are {4} it should take ~ {5} minutes fo finish".format(
        args['from_date'],
        args['to_date'],
        args['path'],
        args['to_path'],
        total_files_to_move,
        round(total_files_to_move/60)
    )
    print(to_print)
    console.append(to_print)
    now = datetime.datetime.now()
    end = now + datetime.timedelta(seconds=total_files_to_move)
    now = now.strftime("%m-%d-%Y %I:%M %p")
    end = end.strftime("%m-%d-%Y %I:%M %p")
    to_print = "The move script started at {0} and it's expected to finish at {1}".format(now,end)
    print(to_print)
    console.append(to_print)
    save_log(console,"console_move")
    for file in files_to_move:
        status_code = 0
        tries = 0 
        while status_code != 200:
            to_path = "{0}/{1}".format(args['to_path'],file['name'])
            jresponse, status_code, response = move(args['api_key'], file['path_lower'], to_path)
            if status_code == 200:
                log_human.append("Moved file from {0} to {1}\n".format(file['path_lower'],to_path))
                log.append(jresponse)
            elif "too_many_write_operations" in jresponse['error_summary']:
                tries += 1
                print("Dropbox throttle, trying again for file {0} try {1}".format(file['name'],tries))
            else:
                error.append(jresponse)
                error.append(file)
                status_code = 200
                #response.raise_for_status()
    save_log(log,"move_files_raw")
    save_log(log_human,"move_files")
    save_log(error,"error")
    to_print = "The move script finished at {0}".format(datetime.datetime.now().strftime("%m-%d-%Y %I:%M %p"))
    print(to_print)
    console.append(to_print)
    save_log(console,"console_move")

def save_log(args,config_name):
    with open("log/{}.log".format(config_name),'w+') as jsonfile:
        json.dump(args, jsonfile, indent=4, separators=(',', ': '), sort_keys=True)

def range_date(date, from_date, to_date):
    try:
        #date2 = datetime.datetime.strptime(date,"%Y-%m-%dT%H:%M:%SZ")
        date2 = datetime.datetime.strptime(date[:10],"%Y-%m-%d")
    except:
        return False
    from_date2 = datetime.datetime.strptime(from_date,"%Y%m%d")
    to_date2 = datetime.datetime.strptime(to_date,"%Y%m%d")
    if from_date2 <= date2 <= to_date2:
        return True
    else:
        return False

def move(api_key,from_path,to_path):
    url = "https://api.dropboxapi.com/2/files/move_v2"
    payload = {
        "from_path" : from_path,
        "to_path" : to_path
        }
    headers = {
        'Content-Type': 'application/json',
        'Authorization': "Bearer {0}".format(api_key)
        }
    response = requests.request("POST", url, headers = headers, data = json.dumps(payload))
    jresponse = json.loads(response.content)
    status_code = response.status_code
    return jresponse, status_code, response
    
def extract(api_key, path, recursive, limit):
    results = []
    folders = []
    files = []
    unknown = []
    console = []
    iteration = 1
    has_more = True
    cursor = ""
    console.append("Extract started at : {0}".format(datetime.datetime.now().strftime("%m-%d-%Y %I:%M %p")))
    while has_more:
        if iteration == 1:
            response = list_folder(api_key, path, recursive, limit)
        else:
            response = list_folder_continue(api_key, cursor)
        has_more = response['has_more']
        cursor = response['cursor']
        results += response['entries']
        categories = categorize(response['entries'])
        folders += categories['folders']
        files += categories['files']
        unknown += categories['unknown']
        status = build_list(
            iteration,
            len(results),
            len(folders),
            len(files),
            len(unknown)
            )
        console.append(print_extract_status(status))
        iteration += 1
    console.append("Finished at {1}, {0} Elements were returned".format(len(results),datetime.datetime.now().strftime("%m-%d-%Y %I:%M %p")))
    save_output(results,"results")
    save_output(folders,"folders")
    save_output(files,"files")
    save_output(unknown,"unknown")
    save_log(console,"console_extract")

def print_extract_status(status):
    string = "Iteration {0:6d} ocurred\t\
            {1:6d} elements\t\
            {2:6d} folders\t\
            {3:6d} files\t\
            {4:6d} unknown so far..."\
            .format(status[0],status[1],status[2],status[3],status[4])
    print(string)
    return string

def build_list(*args):
    elements = []
    for element in args:
        elements.append(element)
    return elements

def categorize(results):
    folders = []
    files = []
    unknown = []
    for element in results:
        if element['.tag'] == "folder":
            folders.append(element)
        elif element['.tag'] == "file":
            files.append(element)
        else:
            unknown.append(element)
    jresponse = {
        'folders': folders,
        'files': files,
        'unknown': unknown
    }
    return jresponse

def save_output(args,config_name):
    with open("tmp/{}.json".format(config_name),'w+') as jsonfile:
        json.dump(args, jsonfile, indent=4, separators=(',', ': '), sort_keys=True)

def load_json_file(config_name):
    with open("tmp/{}.json".format(config_name)) as jsonfile:
        return json.load(jsonfile)

def list_folder_continue(api_key, cursor):
    url = "https://api.dropboxapi.com/2/files/list_folder/continue"
    payload = {
        "cursor": cursor
        }
    headers = {
        'Content-Type': 'application/json',
        'Authorization': "Bearer {0}".format(api_key)
        }
    response = requests.request("POST", url, headers = headers, data = json.dumps(payload))
    jresponse = json.loads(response.content)
    return jresponse

def list_folder(api_key, path, recursive, limit):
    url = "https://api.dropboxapi.com/2/files/list_folder"
    payload = {
        "path": path,
        "recursive": recursive,
        "include_media_info": True,
        "include_deleted": False,
        "include_has_explicit_shared_members": False,
        "include_mounted_folders": True,
        "include_non_downloadable_files": True,
        "limit": limit
        }
    headers = {
        'Content-Type': 'application/json',
        'Authorization': "Bearer {0}".format(api_key)
        }
    response = requests.request("POST", url, headers = headers, data = json.dumps(payload))
    jresponse = json.loads(response.content)
    return jresponse
    
if __name__ == '__main__':
    main()