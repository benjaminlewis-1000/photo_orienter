#! /usr/bin/env python2

import sqlite3
from sqlite3 import Error
import os
import time
import argparse
import Queue
import xmltodict
import multithread_walk as mtw
# UTF encoding for files
from time import sleep
import cv2
import sys  
from populate import tableMinder
import threading


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

# conn = sqlite3.connect('alignDatabase.db')
# c = conn.cursor()
# CLEAR='''UPDATE photos set bighash = NULL'''
# c.execute(CLEAR)
# CLEAR='''UPDATE photos set smallhash = NULL'''
# c.execute(CLEAR)
# conn.commit()
# conn.close()

next_files_q = Queue.Queue()

ii = 0
for file in list_all:
	# print(file)
	# if not os.path.exists(file):
	# 	# print(file)
	# 	db_class.delete_file(file)
	# else:
	# 	# db_class.add_hash(file)
	next_files_q.put(file)
	ii += 1

	if ii % 1000 == 0:
		print(ii)
	# if ii > 100:
	# 	break
	# sleep(1)

print('he')

stop_event = threading.Event()
def append(thread_num):

	jj = 0
	db_class_sub = tableMinder(params, print_inserts = False)
	while not stop_event.is_set():
		if not next_files_q.empty():
			next_file = next_files_q.get()
			if not os.path.exists(next_file):
				db_class_sub.delete_file(next_file)
			else:
				db_class_sub.add_hash(next_file)
			# print next_file
			jj += 1
			if jj % 10 == 0:
				print("Thread {}: {}".format(thread_num, jj) )
		else:
			stop_event.set()
	print('done append')

threads = []

for algo in range(7):
    t = threading.Thread(target=append, args=(algo,))
    threads.append(t)
    t.start()