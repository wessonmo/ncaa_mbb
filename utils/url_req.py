import re
import requests
from requests.exceptions import ConnectionError, ConnectTimeout, ReadTimeout
import time


def url_req(url):
    header = {'User-Agent': ('Mozilla/5.0 (Windows NT 6.1)'
                             'AppleWebKit/537.2 (KHTML, like Gecko)'
                             'Chrome/22.0.1216.0 Safari/537.2')}
    
    for i in range(5):
        try:
            req = requests.get(url, headers = header, timeout = 5 + i*5)
            if re.compile('access denied', re.I).search(req.content):
                del req
                time.sleep(300)
                continue
            elif re.compile('internal server error', re.I).search(req.content):
                del req
                time.sleep(15)
                continue
            break
        except (ConnectionError, ConnectTimeout, ReadTimeout) as to:
            if i == 4:
                raise to
            else:
                continue
     
    return req.content