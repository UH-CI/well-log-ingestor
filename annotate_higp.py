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

#This code only works for HIGP data that has been freshly imported and not examined - the first time you examine a file using the Ike Wai Gateway a File metadata object is create - so those examined files will have two File metadata objects - you would have to remove the First one.
#Also note that the generation of the timestamp is dependent on the folder location in the path - might be a way to use negative numbers in indexes

#define the tenant specific schema uuids - development will be differnet from production
file_schema_uuid = '8458254947603443225-242ac1110-0001-013'
data_descriptor_schema_uuid = '2546299950287950311-242ac1110-0001-013'
published_file_schema_uuid = '2489337622298169831-242ac11f-0001-013'
#define the metadata uuid for the data descriptor to associate with each file
data_descriptor_id = '5818216270609313305-242ac11f-0001-012'

#define the Island and what directory structure (needs to exist on the system already) to store in and system
island = 'Maui'
dirs = ['/Well_Data/'+island]
system = 'ikewai-annotated-data'

files = pd.DataFrame(columns=('url', 'uuid'))
wells = pd.DataFrame(columns=('wid', 'uuid'))
files_array = []
global_counter = 0


def update_well_metadata(fileuuid, well):
    body = well
    print('WellUUID: '+well['uuid'])
    body['value']['published'] = 'True'
    associations = body['associationIds']
    associations.append(fileuuid)
    associations = set(associations)
    body['associationIds'] = list(associations)
    with open('higp_data/well_'+fileuuid+'.json', 'w') as outfile:
        json.dump(body, outfile)
    proc = subprocess.Popen("metadata-addupdate -F higp_data/well_"+fileuuid+".json "+well['uuid'], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    proc.wait()
    p_res= proc.communicate()[0]
    res = p_res.splitlines()[0]
    #p_res, err = p.communicate()
    print("Well :" + res + " MORE NOTHING")
    return "File: "+res

# create the File metadata object after the DataDescriptor object has been created
def create_file_metadata(fileuuid, data_descriptor_uuid, new_data_descriptor_uuid, path, well_uuid):
    body = {}
    body['name'] = 'File';
    values= {};
    #values['file-uuid'] = fileuuid
    #values['filename'] = path
    #values['path'] = path
    body['schemaId'] = file_schema_uuid
    body['value'] = {}#values
    body['associationIds'] = [fileuuid, data_descriptor_uuid, well_uuid, new_data_descriptor_uuid]
    body['permissions'] = [{"username":"jgeis","permission":"ALL"},{"username":"ouida","permission":"ALL"},{"username":"ikewai-admin","permission":"ALL"},{"username":"ikeadmin","permission":"ALL"},{"username":"public","permission":"READ"}]
    with open('higp_data/fm_'+fileuuid+'.json', 'w') as outfile:
        json.dump(body, outfile)
    process = subprocess.Popen("metadata-addupdate -F "+'higp_data/fm_'+fileuuid+'.json', shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    process.wait()
    p_res= process.communicate()[0]
    res = p_res.splitlines()[0]
    #p_res, err = p.communicate()
    print("PROCES :" + p_res + " MORE NOTHING")
    return "File: "+res

# create the File metadata object after the DataDescriptor object has been created
def create_published_file_metadata(fileuuid, data_descriptor_uuid, new_data_descriptor_uuid, filemetadata_uuid, path, well_uuid):
    body = {}
    body['name'] = 'PublishedFile';
    values= {};
    values['filename'] = path
    values['path'] = path
    values['file-uuid'] = fileuuid
    values['published'] = True
    body['schemaId'] = published_file_schema_uuid
    body['value'] = values
    body['associationIds'] = [fileuuid, data_descriptor_uuid, filemetadata_uuid, well_uuid, new_data_descriptor_uuid]
    body['permissions'] = [{"username":"jgeis","permission":"ALL"},{"username":"ouida","permission":"ALL"},{"username":"ikewai-admin","permission":"ALL"},{"username":"ikeadmin","permission":"ALL"},{"username":"public","permission":"READ"}]
    with open('higp_data/pub_'+fileuuid+'.json', 'w') as outfile:
        json.dump(body, outfile)
    exists = 0
    while exists == 0:
        check = subprocess.Popen("metadata-list "+ filemetadata_uuid, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        check.wait()
        check_res= check.communicate()[0]
        print("CHECK: "+check_res)
        if "No metadata item found" not in check_res:
            exists =1
    process = subprocess.Popen("metadata-addupdate -F "+'higp_data/pub_'+fileuuid+'.json', shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    process.wait()
    #p_res= process.communicate()[0]
    p_res, err = process.communicate()
    print("PUBFILE PROCESS :" + p_res + " MORE NOTHING")
    print(err)
    res = p_res.splitlines()[0]

    return "PublishedFile: "+res



#create the data descriptor first so we can associate it with the File metadata object
# NOTE we could add a check to see if a File object exists and if so skip - but this will slow things down
def create_data_descriptor(url, fileuuid, well_uuid, data_descriptor):
    path = url.split(system)[1]
    body={}
    body['name'] = "DataDescriptor"
    body['schemaId'] = data_descriptor['schemaId']
    body['name'] = data_descriptor['name']
    body['associationIds'] = [fileuuid,well_uuid]
    #values['title'] = "Well Data: " + path
    # values['creators'] = [{"first_name" : "Nicole","last_name" : "Lautze","uuid" : "4506932177003015705-242ac1110-0001-012"}]
    # values['license_rights'] = '0'
    # values['license_permission'] = 'public'
    # values['subject'] = "Well Data"
    # values['published'] = "True";
    body['value'] = data_descriptor['value']
    body['value']['title'] = "Well Data: " + path
    body['value']['published'] = 'True'
    body['permissions'] = [{"username":"jgeis","permission":"ALL"},{"username":"public","permission":"read"},{"username":"ouida","permission":"ALL"},{"username":"ikewai-admin","permission":"ALL"},{"username":"ikeadmin","permission":"ALL"}]
    with open('higp_data/dd_'+fileuuid+'.json', 'w') as outfile:
        json.dump(body, outfile)
    process = subprocess.Popen("metadata-addupdate -F "+'higp_data/dd_'+fileuuid+'.json', shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    p_res= process.communicate()[0]
    process.wait()
    res = p_res.splitlines()[0]
    data_descriptor_uuid  = res.split('object')[1].replace('\n','')
    #For some odd reason the data_descriptor_uuid comes back strange from the create and fetching it from the object list seems to work when passing it to the create_file_metadata function without doing this it won't create
    process = subprocess.Popen("metadata-list -V "+ data_descriptor_uuid, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    pd_res= process.communicate()[0]
    process.wait()
    data_descriptor_uuid = json.loads(pd_res)['result']['uuid']
    print('DataDescriptor UUID: '+ data_descriptor_uuid)
    #file_create_result = create_file_metadata(fileuuid, data_descriptor_uuid, path, well_uuid)
    #filemeta_uuid = file_create_result.split('object')[1].replace('\n','')
    return data_descriptor_uuid

def create_annotations(url, data_descriptor, fileuuid, well):
    path = url.split(system)[1]
    well_uuid = well['uuid']
    data_descriptor_uuid = data_descriptor['uuid']
    new_data_descriptor_uuid = create_data_descriptor(url, fileuuid, well_uuid, data_descriptor);
    file_create_result = create_file_metadata(fileuuid, data_descriptor_uuid, new_data_descriptor_uuid,path, well_uuid)
    filemetadata_uuid = file_create_result.split('object ')[1].replace('\n','')
    print("FILEMETADATA:"+ filemetadata_uuid)
    pubfile_create_result = create_published_file_metadata(fileuuid, data_descriptor_uuid, new_data_descriptor_uuid, filemetadata_uuid, path, well_uuid)
    pubfilemeta_uuid = pubfile_create_result.split('object ')[1].replace('\n','')
    update_well_metadata(fileuuid,well)
    return [data_descriptor_uuid,pubfilemeta_uuid]

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
# def fetch_file_info(files):
#     while (len(dirs) > 0):
#         current_dir = dirs.pop()
#         dir_listing = subprocess.check_output("files-index -V -S "+system+current_dir+" -l 2000 --filter=_links,uuid", shell=True)
#         #print(dir_listing)
#         json_object = json.loads(dir_listing)
#         for result in json_object['result']:
#             if result['type'] == 'dir':
#               if result['_links']['self']['href'].split(system)[1] != current_dir:
#                 dirs.append(result['_links']['self']['href'].split(system)[1])
#             else:
#               files = files.append({'url':[result['_links']['self']['href']], 'uuid':[result['uuid']]}, ignore_index=True)
#     return files

def fetch_file_info(offset):
    file_array =[]
    #print('OFFSET: '+str(offset))
    submit_str = "files-list -V -S "+system+'/Well_Data/'+island+" -l 100 -o "+str(offset)+" --filter=_links,uuid"
    #print submit_str
    file_list = subprocess.check_output(submit_str, shell=True)
    #print(dir_listing)
    json_object = json.loads(file_list)['result']
    #print("LENGTH" + str(len(json_object)))
    #for result in json_object['result']:
    for i in range(0,len(json_object)):
    #        if result['type'] == 'dir':
    #      if result['_links']['self']['href'].split(system)[1] != current_dir:
    #        dirs.append(result['_links']['self']['href'].split(system)[1])
    #    else:
      file_array.append(json_object[i]['_links']['self']['href'])
      #files = files.append({'url':[result['_links']['self']['href']], 'uuid':[result['uuid']]}, ignore_index=True)
    #print("FileArray LENGTH" + str(len(file_array)))
    if len(file_array) == 100:
        file_array = file_array + fetch_file_info(offset+100)
    return file_array

def fetch_file_uuid(path):
    json_result = subprocess.check_output("files-index -V -S "+system+path+" --filter=uuid", shell=True)
    file_uuid = json.loads(json_result)['result'][0]['uuid']
    return file_uuid

#Generate a dataframe with urls and uuids for everyfile in the subtree
def fetch_wells(island):
    wells = pd.DataFrame(columns=('wid', 'uuid'))
    #well_listing = subprocess.check_output("metadata-list -V -Q \"{'name':'Well','value.island':'"+island+"'}\" --filter uuid,value.wid -l 5000", shell=True)
    well_listing = subprocess.check_output("metadata-list -V -Q \"{'name':'Well','value.island':'"+island+"'}\" --filter uuid,value,associationIds -l 5000", shell=True)
    json_object = json.loads(well_listing)
    for result in json_object['result']:
      wells = wells.append({'wid':result['value']['wid'], 'uuid':result['uuid'],'value':result['value'],'associationIds':results['associationIds']}, ignore_index=True)
    return wells

#Generate a dataframe with urls and uuids for everyfile in the subtree
def fetch_well(wid):
    well_listing = subprocess.check_output("metadata-list -V -Q \"{'value.wid':'"+wid+"'}\"  -l 1", shell=True)
    well = json.loads(well_listing)['result'][0]
    return well

def fetch_data_descriptor(uuid):
    print("Fetching Data Descriptor")
    results = subprocess.check_output("metadata-list -V -Q \"{'uuid':'"+uuid+"'}\"  -l 1", shell=True)
    data_descriptor = json.loads(results)['result'][0]
    return data_descriptor

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

def check_file(fileuuid):
    process = subprocess.Popen("metadata-list -Q \"{'associationIds':'"+fileuuid+"'}\"", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    process.wait()
    p_res, err = process.communicate()
    print("PUBFILE PROCESS :" + p_res.splitlines()[0] + " MORE NOTHING")
    res = True
    if p_res.splitlines()[0] == '':
        res = False
    return res
#myfiles = fetch_file_info(files)

#mywell.loc[mywell['wid'] == '3-3605-025','uuid'].iloc[0]

#mywells = fetch_wells(island)

#print('Fetching Wells...')
#mywells = fetch_wells(island)
print("Fetching File info...")
myfiles = fetch_file_info(0)
print('myfiles-length: '+ str(len(myfiles)))
#print myfiles
# for i in range(0,10): #len(myfiles)):
#      path = myfiles.iloc[i].url[0].split(system)[1]
#      well_uuid = mywells.loc[mywells['wid'] == path.split('/well_data/')[1].replace('.pdf','').replace('_map','').replace('_misc',''),'uuid'].iloc[0]
#      uuids = create_annotations(myfiles.iloc[i].url[0],data_descriptor_uuid, myfiles.iloc[i].uuid[0],well_uuid)
data_descriptor = fetch_data_descriptor(data_descriptor_id)
print("Annotating Files ...")
for i in range(0,len(myfiles)):
     #path = myfiles.iloc[i].url[0].split(system)[1]
     print(myfiles[i])
     path = myfiles[i].split(system)[1]
     print path
     file_uuid = fetch_file_uuid(path)
     print(file_uuid)
     #check = check_file(myfiles.iloc[i].uuid[0])
     check = check_file(file_uuid)
     print("CHECK: "+str(check))
     if check != True:
         print('add')
         if len(path.split(island+'/')) > 1 :
           wid = path.split(island+'/')[1].replace('.pdf','').replace('_map','').replace('_misc','')
           well = fetch_well(path.split(island+'/')[1].replace('.pdf','').replace('_map','').replace('_misc','').replace('_log',''))
           if well:
             well_uuid = well['uuid']

             #print(data_descriptor)
             #well_uuid = mywells.loc[mywells['wid'] == path.split(island+'/')[1].replace('.pdf','').replace('_map','').replace('_misc',''),'uuid'].iloc[0]


             uuids = create_annotations(myfiles[i],data_descriptor,file_uuid,well)
     else:
         print('skipping')
