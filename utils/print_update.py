from __future__ import print_function
import sys


def print_update(file_action, completed, total):
    template = '\t{0: >6} : {1: >7}/{2: <7}'
    string = template.format(file_action, completed, total)
    
    sys.stdout.flush()
    print(string, end = '\r')