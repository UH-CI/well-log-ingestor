# Import required modules
import httplib2
from bs4 import BeautifulSoup
from subprocess import call

base_url = 'https://www.higp.hawaii.edu/hggrc/wells/Oahu/'
http = httplib2.Http()
status, response = http.request(base_url)

soup = BeautifulSoup(response, 'html.parser')

for link in soup.find_all('a'):
    print(link.get('href').split('/')[1])
    call("files-import -U "+base_url+link.get('href')+" -V -S ikewai-working-sean -z 832d5f4ae24d6e8576cbbf21a9f15 -N "+link.get('href').split('/')[1], shell=True)
