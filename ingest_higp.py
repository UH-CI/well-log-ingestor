# Import required modules
import httplib2
from bs4 import BeautifulSoup
import subprocess
from subprocess import call

island = 'Niihau'
base_url = 'https://www.higp.hawaii.edu/hggrc/wells/'
system = 'ikewai-annotated-data'
access_token ='AGAVE-TOKEN'

http = httplib2.Http()
status, response = http.request(base_url+"/"+island+"/")
print(response)
soup = BeautifulSoup(response, 'html.parser')

for link in soup.find_all('a'):
    print(link.get('href'))
    print(link.get('href').split('/')[1])
    print subprocess.check_output("files-import -U "+base_url+island+"/"+link.get('href')+" -V -S "+system+"/Well_Data/"+island+" -z "+access_token+" -N "+link.get('href').split('/')[1], shell=True)
