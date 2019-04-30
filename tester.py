#! /usr/bin/env python2
# -*- coding: utf-8 -*-

import unittest
import tempfile
from populate import tableMinder
import os
import xmltodict
import sqlite3

import sys  
reload(sys)
sys.setdefaultencoding('utf8')

class test_populator(unittest.TestCase):

    def setUp(self):
        script_path  = os.path.abspath(os.path.join(__file__,".."))

        with open(os.path.join(script_path, 'params.xml') ) as stream:
            try:
                self.params = xmltodict.parse(stream.read())
            except Exception as exc:
                print(exc)
                exit(1)

        self.db_class = tableMinder(self.params, print_inserts = False)

        self.tmp_dir_for_test = tempfile.mkdtemp()

        self.db_class.conn = sqlite3.connect(os.path.join(self.tmp_dir_for_test, self.db_class.db_name))
        self.db_class.conn.text_factory = lambda x: unicode(x, "utf-8", "ignore")
        self.db_class.conn.row_factory = lambda cursor, row: row[0]

        self.pic_root = '.'

        self.tconn = self.db_class.conn
        self.cur = self.tconn.cursor()
        self.db_class.init_table()


        td_raw = [ 'tests/çÃba', 'tests', 'notadir']
        self.test_dirs = []
        for dname in td_raw:
            try:
                dname = dname.encode('utf-8')
            except UnicodeDecodeError as ude:
                pass
            dname = u'' + dname
            self.test_dirs.append(dname)

        self.actual_test_dir = os.path.abspath('tests')
        self.dup_test_dir = os.path.abspath('tests_duplicates')

        multitest_raw = [ 'tests/çÃba', 'tests']
        self.multi_test_dir = []
        for dname in multitest_raw:
            try:
                dname = dname.encode('utf-8')
            except UnicodeDecodeError as ude:
                pass
            dname = u'' + dname
            dname = os.path.abspath(dname)
            self.multi_test_dir.append(dname)

    def test_checkInitTable(self):

        # Test init_table again
        self.db_class.init_table()
        table_names = [self.params['params']['photo_table']['name'], self.params['params']['param_table']['name'], self.params['params']['roots_table']['name']]
        # Check the names of the tables

        name_query = '''SELECT name FROM sqlite_master WHERE type=\'table\''''
        names = self.cur.execute(name_query).fetchall()

        self.assertEqual(set(names), set(table_names))

        # Check columns in each table
        # for table in table_names:
        #     for row in self.cur.execute("pragma table_info('photos')").fetchall():
        #         print row

        # Check empty tables
        for table in table_names:

            num_record_query = '''SELECT COUNT(*) FROM {table}'''.format(table=table)
            num_records = self.cur.execute(num_record_query).fetchall()[0]
            self.assertEqual(num_records, 0) # Check that the other tests don't affect this 


    def test_encodings_roots(self):
        self.db_class.insert_root(self.test_dirs[0])
        self.db_class.insert_root(self.test_dirs[0])
        num_record_query = '''SELECT COUNT(*) FROM {table}'''.format(table=self.params['params']['roots_table']['name'])
        num_records = self.cur.execute(num_record_query).fetchall()[0]
        self.assertEqual(num_records, 1)

        self.db_class.insert_root(self.test_dirs[1])
        num_records = self.cur.execute(num_record_query).fetchall()[0]
        self.assertEqual(num_records, 2)

        self.db_class.insert_root(self.test_dirs[2])
        num_records = self.cur.execute(num_record_query).fetchall()[0]
        self.assertEqual(num_records, 2)

        a = self.cur.execute('SELECT * FROM {table}'.format(table=self.params['params']['roots_table']['name'])).fetchall()
        dirs_basenames = [os.path.abspath(u''+x.encode('utf-8')) for x in self.test_dirs]
        
        for each in a:
            self.assertTrue(os.path.isdir(each))
            self.assertTrue(each.encode('utf-8') in dirs_basenames)

    def __get_num_photos_inserted__(self):
        get_num_files = '''SELECT COUNT(*) FROM {photo_table}'''.format(photo_table = self.params['params']['photo_table']['name'])
        num_inserted = self.cur.execute(get_num_files).fetchall()[0]

        return num_inserted

    def __get_jpgs_under_dir__(self, dirname):
        all_files = []
        print dirname.encode('utf-8')
        self.assertTrue(os.path.isdir(dirname))
        # If you pass a unicode (u''+name) to os.walk, you get unicode results.
        for root, dirs, files in os.walk(u''+dirname):
            # print "Root is {}".format(root)
            # print "Files are {}".format(files)
            for f in files:
                full_path = os.path.join(root, f)
                # print(full_path.lower())
                # print(full_path.lower().endswith(('.jpg', '.jpeg')))
                if os.path.isfile(full_path) and full_path.lower().endswith(('.jpg', '.jpeg')):
                    all_files.append(full_path)#.encode('utf-8'))
        
        # print("File length is {}".format(len(all_files)))
        # print all_files
        return all_files

    def test_add_files(self):
        self.db_class.insert_root(self.actual_test_dir)

        num_record_query = '''SELECT COUNT(*) FROM {table}'''.format(table=self.params['params']['roots_table']['name'])
        num_records = self.cur.execute(num_record_query).fetchall()[0]
        self.assertEqual(num_records, 1) # Check that the other tests don't affect this 

        all_files = self.__get_jpgs_under_dir__(self.actual_test_dir)

        self.db_class.add_from_roots()
        self.db_class.add_from_roots()

        num_inserted = self.__get_num_photos_inserted__()
        self.assertEqual(num_inserted, len(all_files))
        # print num_inserted

        get_stored_files = '''SELECT {file_col} FROM {photo_table}'''.format(photo_table = self.params['params']['photo_table']['name'], \
            file_col=self.params['params']['photo_table']['full_path_col'])
        files_stored = self.cur.execute(get_stored_files).fetchall()

        for f in files_stored:
            self.assertTrue(os.path.isfile(f))

        self.assertEqual(set(files_stored), set(all_files))

    def test_twoRoots(self):
        for dir_under_test in self.multi_test_dir:
            # print("{}".format(dir_under_test))
            self.db_class.insert_root(dir_under_test)
            self.db_class.add_from_roots()
            num_inserted = self.__get_num_photos_inserted__()
            print(dir_under_test)
            jpegs_under = self.__get_jpgs_under_dir__(dir_under_test)

            get_stored_files = '''SELECT {file_col} FROM {photo_table}'''.format(photo_table = self.params['params']['photo_table']['name'], \
                file_col=self.params['params']['photo_table']['full_path_col'])
            files_stored = self.cur.execute(get_stored_files).fetchall()
            # print jpegs_under
            self.assertGreaterEqual(num_inserted, len(jpegs_under))
            # print set(jpegs_under) 
            # print set(files_stored)
            self.assertTrue(len(set(jpegs_under) - set(files_stored)) == 0)

        num_stored_query = '''SELECT COUNT(*) FROM {photo_table}'''.format(photo_table = self.params['params']['photo_table']['name'])
        num_files_stored = self.cur.execute(num_stored_query).fetchall()

        for dir_under_test in self.multi_test_dir:
            self.db_class.insert_root(dir_under_test)
            self.db_class.add_from_roots()
            num_inserted = self.__get_num_photos_inserted__()
            jpegs_under = self.__get_jpgs_under_dir__(dir_under_test)

            get_stored_files = '''SELECT {file_col} FROM {photo_table}'''.format(photo_table = self.params['params']['photo_table']['name'], \
                file_col=self.params['params']['photo_table']['full_path_col'])
            files_stored = self.cur.execute(get_stored_files).fetchall()
            # print jpegs_under
            self.assertGreaterEqual(num_inserted, len(jpegs_under))
            self.assertTrue(len(set(jpegs_under) - set(files_stored)) == 0)
            new_num = self.cur.execute(num_stored_query).fetchall()
            # Assert that no new files were stored.
            self.assertEqual(new_num, num_files_stored)

        # Number of stored files should be the same as the list of unprocessed files' length.
        unprocessed_list = self.db_class.list_unprocessed()
        under_test_dir = self.__get_jpgs_under_dir__(self.actual_test_dir)
        self.assertTrue(len(unprocessed_list), num_files_stored)
        self.assertTrue(set(unprocessed_list) == set(under_test_dir))


    def test_add_duplicates(self):
        self.db_class.insert_root(self.dup_test_dir)
        self.db_class.add_from_roots()

        num_record_query = '''SELECT COUNT(*) FROM {table}'''.format(table=self.params['params']['photo_table']['name'])
        num_records = self.cur.execute(num_record_query).fetchall()[0]
        print(num_records)
        self.assertEqual(num_records, 1) 

    def test_move_file(self):
        self.db_class.insert_root(self.actual_test_dir)
        self.db_class.add_from_roots()
        unprocessed_list = self.db_class.list_unprocessed()
        old_len = len(unprocessed_list)

        change_file = unprocessed_list[0]
        parts = change_file.split('/')
        newname = os.path.join('/'.join(parts[:-1]), 'garbage_nobody.jpg')
        # print newname
        os.rename(change_file, newname)
        self.db_class.add_from_roots()
        os.rename(newname, change_file)
        unprocessed_list2 = self.db_class.list_unprocessed()
        self.assertEqual(len(unprocessed_list2), old_len)
        # print  unprocessed_list2
        self.assertTrue(newname in unprocessed_list2)
        self.assertFalse(change_file in unprocessed_list2)


    def test_mark_processed(self):
        # Set up database, then get list of unprocessed files
        self.db_class.insert_root(self.actual_test_dir)
        self.db_class.add_from_roots()
        unprocessed_list = self.db_class.list_unprocessed()

        # Do a 'None' action on all the unprocessed files
        self.assertTrue(len(unprocessed_list) > 0)
        for file in unprocessed_list:
            self.db_class.mark_processed(file, action=None)

        unprocessed_list = self.db_class.list_unprocessed()
        self.assertTrue(len(unprocessed_list) == 0)

        # Test inserting each of the possible actions. 
        poss_actions = self.params['params']['possible_actions']
        poss_actions = [poss_actions[key] for key in poss_actions.keys()]
        for action in poss_actions:
            # Reset the table
            self.db_class.init_table()
            self.db_class.insert_root(self.actual_test_dir)
            self.db_class.add_from_roots()
            unprocessed_list = self.db_class.list_unprocessed()

            self.assertTrue(len(unprocessed_list) > 0)
            for file in unprocessed_list:
                self.db_class.mark_processed(file, action=action)

            unprocessed_list = self.db_class.list_unprocessed()
            self.assertTrue(len(unprocessed_list) == 0)

        # Test a bad action
        self.db_class.init_table()
        self.db_class.insert_root(self.actual_test_dir)
        self.db_class.add_from_roots()
        unprocessed_list = self.db_class.list_unprocessed()
        start_length = len(unprocessed_list)
        self.db_class.mark_processed(unprocessed_list[0], action='zzzyx_not_an_action')
        unprocessed_list = self.db_class.list_unprocessed()
        self.assertEqual(start_length, len(unprocessed_list))

    def test_sequence_of_actions(self):
        self.db_class.insert_root(self.actual_test_dir)
        self.db_class.add_from_roots()
        unprocessed_list = self.db_class.list_unprocessed()


        poss_actions = self.params['params']['possible_actions']
        none_action = poss_actions['action_none']
        clockwise_action = poss_actions['action_clockwise']
        ccw_action = poss_actions['action_ccw']
        r180_action = poss_actions['action_180']
        delete_action = poss_actions['action_delete']

        test_file = unprocessed_list[0]

        def __get_net__():
            get_path_query = '''SELECT {action_col} FROM {photo_table} WHERE {full_path_col} = ?'''\
            .format(photo_table = self.db_class.photo_table_name, full_path_col = self.db_class.full_path_col,\
             action_col = self.db_class.action_taken_col)

            action_stored = self.cur.execute(get_path_query, (test_file,)).fetchall()[0]
            return action_stored

        self.db_class.mark_processed(test_file)
        self.assertEqual(__get_net__(), none_action)
        self.db_class.mark_processed(test_file, clockwise_action)
        self.assertEqual(__get_net__(), clockwise_action)
        self.db_class.mark_processed(test_file, clockwise_action)
        self.assertEqual(__get_net__(), r180_action)
        self.db_class.mark_processed(test_file, clockwise_action)
        self.assertEqual(__get_net__(), ccw_action)
        self.db_class.mark_processed(test_file, delete_action)
        self.assertEqual(__get_net__(), delete_action)
        self.db_class.mark_processed(test_file, r180_action)
        self.assertEqual(__get_net__(), r180_action)
        self.db_class.mark_processed(test_file, r180_action)
        self.assertEqual(__get_net__(), none_action)
        self.db_class.mark_processed(test_file, r180_action)
        self.db_class.mark_processed(test_file, r180_action)
        self.assertNotEqual(__get_net__(), delete_action)
        self.db_class.mark_processed(test_file, r180_action)
        self.db_class.mark_processed(test_file)
        self.assertEqual(__get_net__(), r180_action)
        self.db_class.mark_processed(test_file, r180_action)
        self.db_class.mark_processed(test_file, ccw_action)
        self.assertEqual(__get_net__(), ccw_action)
        self.db_class.mark_processed(test_file, ccw_action)
        self.assertEqual(__get_net__(), r180_action)
        self.db_class.mark_processed(test_file, ccw_action)
        self.assertEqual(__get_net__(), clockwise_action)
        self.db_class.mark_processed(test_file, ccw_action)
        self.assertEqual(__get_net__(), none_action)
        self.db_class.mark_processed(test_file, delete_action)
        self.assertEqual(__get_net__(), delete_action)
        self.db_class.mark_processed(test_file, none_action)
        self.assertEqual(__get_net__(), delete_action)

    def test_net_actions(self):
        poss_actions = self.params['params']['possible_actions']

        none_action = poss_actions['action_none']
        clockwise_action = poss_actions['action_clockwise']
        ccw_action = poss_actions['action_ccw']
        r180_action = poss_actions['action_180']
        delete_action = poss_actions['action_delete']

        # Starting with none, should get same action.
        self.assertEqual(self.db_class.__net_action__(none_action, none_action), none_action)
        self.assertEqual(self.db_class.__net_action__(none_action, clockwise_action), clockwise_action)
        self.assertEqual(self.db_class.__net_action__(none_action, ccw_action), ccw_action)
        self.assertEqual(self.db_class.__net_action__(none_action, r180_action), r180_action)
        self.assertEqual(self.db_class.__net_action__(none_action, delete_action), delete_action)

        # Start with delete, should be similar but not same as above.
        self.assertEqual(self.db_class.__net_action__(delete_action, none_action), delete_action)
        self.assertFalse(self.db_class.__net_action__(delete_action, none_action) == none_action)
        self.assertEqual(self.db_class.__net_action__(delete_action, clockwise_action), clockwise_action)
        self.assertEqual(self.db_class.__net_action__(delete_action, ccw_action), ccw_action)
        self.assertEqual(self.db_class.__net_action__(delete_action, r180_action), r180_action)
        self.assertEqual(self.db_class.__net_action__(delete_action, delete_action), delete_action)

        # Start with clockwise action        
        self.assertEqual(self.db_class.__net_action__(clockwise_action, none_action), clockwise_action)
        self.assertEqual(self.db_class.__net_action__(clockwise_action, clockwise_action), r180_action)
        self.assertEqual(self.db_class.__net_action__(clockwise_action, ccw_action), none_action)
        self.assertEqual(self.db_class.__net_action__(clockwise_action, r180_action), ccw_action)
        self.assertEqual(self.db_class.__net_action__(clockwise_action, delete_action), delete_action)
        
        # Start with counter-clockwise action        
        self.assertEqual(self.db_class.__net_action__(ccw_action, none_action), ccw_action)
        self.assertEqual(self.db_class.__net_action__(ccw_action, clockwise_action), none_action)
        self.assertEqual(self.db_class.__net_action__(ccw_action, ccw_action), r180_action)
        self.assertEqual(self.db_class.__net_action__(ccw_action, r180_action), clockwise_action)
        self.assertEqual(self.db_class.__net_action__(ccw_action, delete_action), delete_action)

        # Start with 180 rotation action        
        self.assertEqual(self.db_class.__net_action__(r180_action, none_action), r180_action)
        self.assertEqual(self.db_class.__net_action__(r180_action, clockwise_action), ccw_action)
        self.assertEqual(self.db_class.__net_action__(r180_action, ccw_action), clockwise_action)
        self.assertEqual(self.db_class.__net_action__(r180_action, r180_action), none_action)
        self.assertEqual(self.db_class.__net_action__(r180_action, delete_action), delete_action)


if __name__ == '__main__':
    unittest.main()
