#! /usr/bin/env python2

import sqlite3
from sqlite3 import Error
import os
import time
import argparse
import xmltodict
import multithread_walk as mtw
# UTF encoding for files
from time import sleep
import cv2
import sys  
from populate import tableMinder



script_path  = os.path.abspath(os.path.join(__file__,".."))

with open(os.path.join(script_path, 'params.xml') ) as stream:
    try:
        params = xmltodict.parse(stream.read())
    except Exception as exc:
        print(exc)
        exit(1)

db_class = tableMinder(params, print_inserts = False)
list_all = db_class.list_unhashed()
print(len(list_all))


ii = 0
for file in list_all:
	# print(file)
	if not os.path.exists(file):
		# print(file)
		db_class.delete_file(file)
	else:
		db_class.add_hash(file)
	ii += 1

	if ii % 10 == 0:
		print(ii)
	# sleep(1)