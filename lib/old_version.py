import requests
import json
import datetime
import time

args = {
    "path": "/Cargas de cámara",
    "to_path": "/Cargas de cámara/2011",
    "from_date": "20110101", # Format YYYYMMDD
    "to_date": "20111231", # Format YYYYMMDD
    "recursive": True,
    "limit": 2000
}
console = []

def main():
    api_dic = load_json_file("conf/api_dic.json")
    level = "files/list_folder/parameters"
    api_dic = set_dict_entry(api_dic,"{0}/path".format(level),args["path"])
    api_dic = set_dict_entry(api_dic,"{0}/limit".format(level),args["limit"])
    api_dic = set_dict_entry(api_dic,"{0}/recursive".format(level),args["recursive"])
    move(api_dic)
    save_output(console, "log/console.json")

def move(api_dic):
    (entries, finished, iteration, wait_time, success, fail) = [[], False, 1, 90, [], []]
    filtered_files = extract(api_dic)
    if folder_exists(api_dic):
        for file in filtered_files:
            entries.append(
                {
                    "from_path": file["path_lower"],
                    "to_path": "{0}/{1}".format(args["to_path"],file["name"])
                }
            )
    api_dic = set_dict_entry(api_dic, "files/move_batch/parameters/entries", entries)
    start = datetime.datetime.now()
    add_log_entry("Move started at : {0}".format(start.strftime("%m-%d-%Y %I:%M %p")))
    while not(finished):
        if iteration == 1:
            response = dropbox_api(api_dic, "files/move_batch")
        else:
            response = dropbox_api(api_dic, "files/move_batch_check")
        if response.ok:
                add_log_entry("Iteration {1:6d} ocurred. Move of {0:6d} files continued successfully!, trying again in {2:6d} seconds ...".format(len(entries), iteration, wait_time))
                jresponse = json.loads(response.content)
                if jresponse[".tag"] == "async_job_id":
                    api_dic = set_dict_entry(api_dic, "files/move_batch_check/parameters/async_job_id", jresponse["async_job_id"])
                    add_log_entry("This is the move job id : {0}".format(jresponse["async_job_id"]))
                    iteration += 1
                    time.sleep(wait_time)
                    continue
                elif jresponse[".tag"] == "in_progress":
                    iteration += 1
                    time.sleep(wait_time)
                    continue
                else:
                    end = datetime.datetime.now()
                    add_log_entry("Move finished at : {0}".format(end.strftime("%m-%d-%Y %I:%M %p")))
                    break
        else:
            add_log_entry("There was a problem with the Dropbox api")
            add_log_entry(json.dumps(jresponse, indent=2))
            break
    td = end - start
    td_mins = int(round(td.total_seconds() / 60))
    add_log_entry("Move took ~ {0} minutes".format(td_mins))
    for i, file in enumerate(jresponse['entries']):
        if "success" in file[".tag"]: success.append(file)
        else:
            file["from_path"] = entries[i]["from_path"]
            file["to_path"] = entries[i]["to_path"]
            fail.append(file)
    add_log_entry("{0} files were succefully moved, {1} failed to move".format(len(success), len(fail)))
    save_output(success, "log/move_success.json")
    save_output(fail, "log/move_fail.json")
    save_output(api_dic, "log/api_dic.json")
    save_output(jresponse, "log/move_response.json")

def add_log_entry(entry):
    console.append(entry)
    print(entry)

def folder_exists(api_dic):
    level = "files/list_folder/parameters"
    api_dic = set_dict_entry(api_dic,"{0}/path".format(level),args["to_path"])
    api_dic = set_dict_entry(api_dic,"{0}/limit".format(level),1)
    api_dic = set_dict_entry(api_dic,"{0}/recursive".format(level),False)
    response = dropbox_api(api_dic, "files/list_folder")
    jresponse = json.loads(response.content)
    if not(response.ok) and ("path/not_found" in jresponse["error_summary"]):
        level = "files/create_folder/parameters"
        api_dic = set_dict_entry(api_dic,"{0}/path".format(level),args["to_path"])
        response = dropbox_api(api_dic, "files/create_folder")
        if response.ok: 
            add_log_entry("Folder {0} created successfully!".format(args["to_path"]))
            return True
        else:
            add_log_entry("Folder {0} unable to create!".format(args["to_path"]))
            add_log_entry(json.dumps(jresponse, indent=2))
            return False
    elif not(response.ok):
        add_log_entry("An error occurred trying to list the folder {0} in the Dropbox API, the error is:".format(args["to_path"])) 
        add_log_entry(json.dumps(jresponse, indent=2))
        return False
    else:
        add_log_entry("Folder {0} already exists!".format(args["to_path"]))
        return True

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
    
def extract(api_dic):
    (results, folders, files, unknown, filtered_files) = [[],[],[],[], []]
    (iteration, has_more) = [1, True]
    start = datetime.datetime.now()
    add_log_entry("Extract started at : {0}".format(start.strftime("%m-%d-%Y %I:%M %p")))
    while has_more:
        if iteration == 1: response = dropbox_api(api_dic, "files/list_folder")
        else: response = dropbox_api(api_dic, "files/list_folder_continue")
        jresponse = json.loads(response.content)
        if not(response.ok):
            add_log_entry(json.dumps(jresponse, indent=2))
            break
        has_more = jresponse['has_more']
        api_dic = set_dict_entry(api_dic,"files/list_folder_continue/parameters/cursor",jresponse["cursor"])
        results += jresponse['entries']
        categories = categorize(jresponse['entries'])
        folders += categories['folders']
        files += categories['files']
        unknown += categories['unknown']
        filtered_files += categories['filtered_files']
        add_log_entry(extract_status([iteration, len(results), len(folders), len(files), len(unknown), len(filtered_files)]))
        iteration += 1
    end = datetime.datetime.now()
    add_log_entry("Finished at {1}, {0} Elements were returned".format(len(results),end.strftime("%m-%d-%Y %I:%M %p")))
    td = end - start
    td_mins = int(round(td.total_seconds() / 60))
    add_log_entry("Extract took ~ {0} minutes".format(td_mins))
    save_output(results, "tmp/results.json")
    save_output(folders, "tmp/folders.json")
    save_output(files, "tmp/files.json")
    save_output(unknown,"tmp/unknown.json")
    save_output(filtered_files,"tmp/filtered_files.json")
    return filtered_files

def extract_status(status):
    string = "Iteration {0:6d} ocurred\
            {1:6d} elements\
            {2:6d} folders\
            {3:6d} files\
            {4:6d} unknown\
            {5:6d} filtered files so far..."\
            .format(status[0],status[1],status[2],status[3],status[4],status[5])
    return string

def categorize(results):
    (folders, files, unknown, filtered_files) = [[], [], [], []]
    for element in results:
        if element['.tag'] == "folder": folders.append(element)
        elif element['.tag'] == "file": 
            files.append(element)
            if range_date(element['name'], args['from_date'], args['to_date']): 
                filtered_files.append(element)
        else: unknown.append(element)
    return {"folders": folders, "files": files, "unknown": unknown, "filtered_files": filtered_files}

def save_output(args, file):
    with open(file,'w+') as jsonfile:
        json.dump(args, jsonfile, indent=4, separators=(',', ': '), sort_keys=True)

def load_json_file(config_file):
    with open(config_file) as jsonfile:
        return json.load(jsonfile)

def dropbox_api(api_dic, level):
    url = get_dict_entry(api_dic, "{0}/url".format(level))
    method = get_dict_entry(api_dic, "{0}/method".format(level))
    parameters = get_dict_entry(api_dic, "{0}/parameters".format(level))
    headers = get_dict_entry(api_dic, "{0}/headers".format(level))
    headers["Authorization"] = "Bearer {0}".format(api_dic["api_key"])
    retry = True
    while retry:
        response = requests.request(method, url, headers = headers, data = json.dumps(parameters))
        jresponse = json.loads(response.content)
        if response.ok:
            break
        elif "too_many_write_operations" in jresponse['error_summary']:
            add_log_entry("Dropbox throttle trying again ...")
            retry = True
        else:
            #print(json.dumps(jresponse, indent=2))
            #response.raise_for_status()
            break
    return response

def get_dict_entry(dictionary, dict_path):
    dict2 = dictionary
    dict_expr = build_dict_entry(dict_path)
    return eval(dict_expr)

def set_dict_entry(dictionary, dict_path, value):
    dict2 = dictionary
    dict_expr = build_dict_entry(dict_path)
    type_value = str(type(value))
    if "str" in type_value: exec("{0} = \"{1}\"".format(dict_expr,value))
    elif any(x in type_value for x in ["int","bool","dict","list"]): exec("{0} = {1}".format(dict_expr,value))
    else: add_log_entry("Error in data type on function set_dict_entry, data type {0} not found".format(type_value))
    return dict2

def build_dict_entry(dict_path):
    elements = dict_path.split("/")
    dict_expr = "dict2"
    for element in elements:
        dict_expr = dict_expr + "[\"{0}\"]".format(element)
    return dict_expr

if __name__ == '__main__':
    main()