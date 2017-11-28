# Import required modules
import httplib2
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import dateutil.parser as parser
import json
import subprocess
from subprocess import call
from subprocess import Popen
from pyproj import Proj, transform
import time
import threading

#This code only works for HIGP data that has been freshly imported and not examined - the first time you examine a file using Ike ToGo a File metadata object is create - so those examined files will have two File metadata objects - you would have to remove the First one.
#Also note that the generate of the timestamp is dependent on the folder location in the path - might be a way to use negative numbers in indexes
site_uuid = ''
var_uuid = ''
file_schema_uuid = '3557207775540866585-242ac1110-0001-013'
data_descriptor_schema_uuid = '4635683822558122471-242ac1110-0001-013'

island = 'Oahu'
dirs = ['/well_data']
files = pd.DataFrame(columns=('url', 'uuid'))
wells = pd.DataFrame(columns=('wid', 'uuid'))
system = 'ikewai-working-sean'
global_counter = 0

# create the File metadata object after the DataDescriptor object has been created
def create_file_metadata(fileuuid, data_descriptor_uuid, path, well_uuid):
    body = {}
    body['name'] = 'File';
    values= {};
    values['filename'] = path
    values['path'] = path
    body['schemaId'] = '3557207775540866585-242ac1110-0001-013'
    body['value'] = values
    body['associationIds'] = [fileuuid, data_descriptor_uuid, well_uuid]
    body['permissions'] = [{"username":"jgeis","permission":"ALL"},{"username":"ouida","permission":"ALL"},{"username":"ikewai-admin","permission":"ALL"},{"username":"ikeadmin","permission":"ALL"}]
    with open('higp_data/'+fileuuid+'.json', 'w') as outfile:
        json.dump(body, outfile)
    process = subprocess.Popen("metadata-addupdate -F "+'higp_data/'+fileuuid+'.json', shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    p_res= process.communicate()[0]
    process.wait()
    res = p_res.splitlines()[0]
    return "File: "+res


#create the data descriptor first so we can associate it with the File metadata object
# NOTE we could add a check to see if a File object exists and if so skip - but this will slow things down
def create_data_descriptor(url, fileuuid, well_uuid):
    path = url.split(system)[1]
    body={}
    values = {}
    body['name'] = "DataDescriptor"
    body['schemaId'] = data_descriptor_schema_uuid
    body['associationIds'] = fileuuid
    values['title'] = "Well Data: " + path
    values['creators'] = [{"first_name" : "Nicole","last_name" : "Lautze","uuid" : "4506932177003015705-242ac1110-0001-012"}]
    values['license_rights'] = '0'
    values['license_permission'] = 'public'
    values['subject'] = "Well Data"
    body['value'] = values
    body['permissions'] = [{"username":"jgeis","permission":"ALL"},{"username":"public","permission":"read"},{"username":"ouida","permission":"ALL"},{"username":"ikewai-admin","permission":"ALL"},{"username":"ikeadmin","permission":"ALL"}]
    with open('higp_data/'+path.split('well_data/')[1].replace('.pdf','')+'.json', 'w') as outfile:
        json.dump(body, outfile)
    process = subprocess.Popen("metadata-addupdate -F "+'higp_data/'+path.split('well_data/')[1].replace('.pdf','')+'.json', shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    p_res= process.communicate()[0]
    process.wait()
    print(fileuuid)
    res = p_res.splitlines()[0]
    data_descriptor_uuid  = res.split('object')[1].replace('\n','')
    #For some odd reason the data_descriptor_uuid comes back strange from the create and fetching it from the object list seems to work when passing it to the create_file_metadata function without doing this it won't create
    process = subprocess.Popen("metadata-list -V "+ data_descriptor_uuid, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    pd_res= process.communicate()[0]
    process.wait()
    data_descriptor_uuid = json.loads(pd_res)['result']['uuid']
    file_create_result = create_file_metadata(fileuuid, data_descriptor_uuid, path, well_uuid)
    filemeta_uuid = file_create_result.split('object')[1].replace('\n','')
    return [data_descriptor_uuid,filemeta_uuid]


#Generate
def set_file_uuids_persistent():
    while (len(dirs) > 0):
        current_dir = dirs.pop()
        dir_listing = subprocess.check_output("files-list -V -S "+system+current_dir, shell=True)
        json_object = json.loads(dir_listing)
        for result in json_object['result']:
            if result['type'] == 'dir':
              if result['_links']['self']['href'].split(system)[1] != current_dir:
                dirs.append(result['_links']['self']['href'].split(system)[1])
            else:
              #list each file item so a persistent uuid is assigned
              subprocess.check_output("files-list -S "+result['_links']['self']['href'].split(system)[1], shell=True)

#Generate a dataframe with urls and uuids for everyfile in the subtree
def fetch_file_info(files):
    while (len(dirs) > 0):
        current_dir = dirs.pop()
        dir_listing = subprocess.check_output("files-index -V -S "+system+current_dir, shell=True)
        json_object = json.loads(dir_listing)
        for result in json_object['result']:
            if result['type'] == 'dir':
              if result['_links']['self']['href'].split(system)[1] != current_dir:
                dirs.append(result['_links']['self']['href'].split(system)[1])
            else:
              files = files.append({'url':[result['_links']['self']['href']], 'uuid':[result['uuid']]}, ignore_index=True)
    return files

#Generate a dataframe with urls and uuids for everyfile in the subtree
def fetch_wells(island):
    wells = pd.DataFrame(columns=('wid', 'uuid'))
    well_listing = subprocess.check_output("metadata-list -V -Q \"{'name':'Well','value.island':'"+island+"'}\" --filter uuid,value.wid -l 5000", shell=True)
    json_object = json.loads(well_listing)
    for result in json_object['result']:
      wells = wells.append({'wid':result['value']['wid'], 'uuid':result['uuid']}, ignore_index=True)
    return wells

def set_permissions(meta_uuid):
    print(meta_uuid)
    subprocess.Popen("metadata-pems-addupdate -u public -p READ" + meta_uuid, shell=True)
    subprocess.Popen("metadata-pems-addupdate -u jgeis -p ALL" + meta_uuid, shell=True)
    subprocess.Popen("metadata-pems-addupdate -u ikewai-admin -p ALL" + meta_uuid, shell=True)
    subprocess.Popen("metadata-pems-addupdate -u omeier -p ALL" + meta_uuid, shell=True)

# Thread worker that runs over a portion of dataframe rows to create metadata
def worker(start, end):
    for i in range(start,end):
        uuids = create_data_descriptor(files.iloc[i].url[0],files.iloc[i].uuid[0])
        set_permissions(uuids[0])
        set_permissions(uuids[1])
    return

# Parallel metadata creation - beware overwhelming the webserver with calls
# expects a dataframe with filepath,uuids and the number of threads to run across
def threaded_create_metadata(files, num_threads):
    threads = []
    offset = len(files)/num_threads
    for i in range(num_threads):
        start_range = offset * i-1
        end_range = offset * i
        if (i == num_threads):
            end_range += len(files) - offset*i
        t = threading.Thread(target=worker, args=(start_range,end_range))
        threads.append(t)
        t.start()

mywells = fetch_wells('Oahu')
myfiles = fetch_file_info(files)

#mywell.loc[mywell['wid'] == '3-3605-025','uuid'].iloc[0]

#mywells = fetch_wells(island)

for i in range(0,len(files)):
     path = url.split(system)[1]
     well_uuid = mywell.loc[mywell['wid'] == path.split('/well_data/').replace('.pdf','').replace('_map','').replace('_misc'),'uuid'].iloc[0]
     uuids = create_data_descriptor(files.iloc[i].url[0],files.iloc[i].uuid[0])
     set_permissions(uuids[0])
     set_permissions(uuids[1])

mywells = fetch_wells('Oahu')
myfiles = fetch_file_info(files)
for i in range(0,len(myfiles)):
     path = myfiles.iloc[i].url[0].split(system)[1]
     well_uuid = mywells.loc[mywells['wid'] == path.split('/well_data/')[1].replace('.pdf','').replace('_map','').replace('_misc',''),'uuid'].iloc[0]
     uuids = create_data_descriptor(myfiles.iloc[i].url[0],myfiles.iloc[i].uuid[0],well_uuid)
