#! /usr/bin/env python

from pynput.keyboard import Key
from pynput.keyboard import Listener
import threading
import Queue
import cv2
import xmltodict
import os
from time import sleep
from populate import tableMinder

file_queue = Queue.Queue()
result_queue = Queue.Queue()

def loadPhoto():
	while True:
		fname = file_queue.get()
		img = cv2.imread(list_of_files[i % num_files])
		result_queue.put([fname, img])


script_path  = os.path.abspath(os.path.join(__file__,"../.."))

with open(os.path.join(script_path, 'params.xml') ) as stream:
    try:
        params = xmltodict.parse(stream.read())
    except Exception as exc:
        print(exc)
        exit(1)

db_class = tableMinder(params, print_inserts = False)

list_of_files = db_class.list_unprocessed()