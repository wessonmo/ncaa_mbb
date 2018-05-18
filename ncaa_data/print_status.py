from __future__ import print_function
import sys

def print_status(file_type, file_action, completed, total, stage):
    template = ' {0: >14} - {1: <6} : {2: >7}/{3: <7}'
    string = template.format(file_type, file_action, completed, total)
    
    if stage < 2:
        if stage == 1: sys.stdout.flush()
        print(string, end = '\r')
    elif stage == 2:
        print(string)