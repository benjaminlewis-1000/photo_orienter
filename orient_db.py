#! /usr/bin/env python
# -*- coding: utf-8 -*-

import sqlite3
from sqlite3 import Error
import os
import time
import argparse
import xmltodict
from populate import tableMinder

# project_path = os.path.abspath(os.path.join(__file__,"../.."))
script_path  = os.path.abspath(os.path.join(__file__,".."))

with open(os.path.join(script_path, 'params.xml') ) as stream:
    try:
        params = xmltodict.parse(stream.read())
    except Exception as exc:
        print(exc)
        exit(1)

# Set up the parameters

parser = argparse.ArgumentParser(description="Python to orient photos")
parser.add_argument('--dbInit', action='store_true', help='Create new database')
parser.add_argument('--root', help='Root in which to search for JPG files')
parser.add_argument('--update_db', action="store_true", help='Update the database')

args = parser.parse_args()

print args

table_object = tableMinder(params)

if args.dbInit:
    table_object.init_table()

if args.root is not None:
    table_object.insert_root(args.root)

if args.update_db:
    table_object.add_from_roots()

unprocessed_files = table_object.list_unprocessed()
if len(unprocessed_files) == 0:
    exit()
print unprocessed_files[3]
