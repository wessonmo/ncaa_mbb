from bs4 import BeautifulSoup
import requests
import time

def soupify(url):
    
    header = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.2 (KHTML, like Gecko) Chrome/22.0.1216.0 Safari/537.2'}
    
    for i in range(5):
        try:
            req = requests.get(url, headers = header)
            break
        except:
            time.sleep(15)
            continue
     
    return BeautifulSoup(req.content, 'lxml')