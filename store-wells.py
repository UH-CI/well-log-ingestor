import pandas as pd
import numpy as np
import dateutil.parser as parser
import json
from subprocess import call
from pyproj import Proj, transform

access_token ='AGAVE-TOKEN'
#p1 = Proj(init='epsg:4269')
#p2 = Proj(init='epsg:4326')
#coord = transform(p1,p2,df.loc[0]['lat83dd'],df.loc[0]['long83dd'])

#read csv well file into dataframe
df1 = pd.read_csv('wells.csv')

#set static json body values and permsissions
#users needs to exist in agave
body={}
pem1={}
pem1['username']= 'seanbc'
pem1['permission']='ALL'
pem2={}
pem2['username']= 'jgeis'
pem2['permission']='ALL'
pem4={}
pem4['username']= 'ikewai-admin'
pem4['permission']='ALL'
pem5={}
pem5['username']= 'public'
pem5['permission']='READ'

body['name'] = "Well"
#the schemaID value needs to match your Well schema object UUID in Agave
body['schemaId'] = "540303726288114151-242ac1110-0001-013"

body['permissions']=[pem1,pem2,pem4,pem5]
#should loop through each dataframe row convert to json and modify to fit well schema
i=0
for i in df1.index:
    j = df1.loc[i].to_json()
    js = json.loads(j)
    if js['wcr'] is not None:
      js['wcr'] = (parser.parse(js['wcr'])).isoformat()
    js['longitude'] = js['long83dd']
    js['latitude'] = js['lat83dd']
    #This stores a GeoJSON object in the value.loc field - in Ike Wai this has a spatial index on it in mongodb
    js['loc'] = {"type":"Point", "coordinates":[js['latitude'],js['longitude']]}
    body['value'] = js
    body['geospatial']= True;
    #write out our json to a file for use with the CLI command
    with open('wells/import-well'+str(i)+'.json', 'w') as outfile:
        json.dump(body, outfile)
    #call the CLI to add our well object
    call("metadata-addupdate -z "+access_token+" -F wells/import-well"+str(i)+".json", shell=True)
