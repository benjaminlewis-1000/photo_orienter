#! /usr/bin/env python

import sqlite3
from sqlite3 import Error
import os
import time
import argparse
import xmltodict
import multithread_walk as mtw
# UTF encoding for files
import cv2
import numpy as np
import sys  
reload(sys)
sys.setdefaultencoding('utf8')

class tableMinder:
    """docstring for tableMinder"""
    def __init__(self, params, print_inserts = True):
        # super(tableMinder, self).__init__()

        # Get the parameters
        self.params = params

        self.print_inserts = print_inserts

        # From the parameter file, get the structure of the database tables.
        self.db_name = params['params']['db_name']
        self.photo_table_name = params['params']['photo_table']['name']
        self.full_path_col = params['params']['photo_table']['full_path_col']
        self.reviewed_col = params['params']['photo_table']['reviewed_col']
        self.action_taken_col = params['params']['photo_table']['action_taken_col']
        self.big_hash_col = params['params']['photo_table']['big_hash_col']
        self.small_hash_col = params['params']['photo_table']['small_hash_col']

        self.param_table = params['params']['param_table']['name']
        self.param_name = params['params']['param_table']['name_col']
        self.param_val = params['params']['param_table']['val_col']

        self.roots_table = params['params']['roots_table']['name']
        self.roots_col = params['params']['roots_table']['root_col']

        # Get a list of possible actions - all three multiple of 90 degree
        # rotations, none, and delete.
        self.poss_actions = params['params']['possible_actions']
        self.none_action = self.poss_actions['action_none']

        # Make the possible actions into a list. It gets the list from
        # the parameter keys. 
        self.poss_actions_list = [self.poss_actions[key] for key in self.poss_actions.keys()]

        # Connect to the database 
        script_path  = os.path.abspath(os.path.join(__file__,".."))
        self.conn = sqlite3.connect(os.path.join(script_path, self.db_name))
        self.conn.text_factory = lambda x: unicode(x, "utf-8", "ignore")
        # Don't do the following -- it messes up if you expect more than one column.
        ## ##  self.conn# # ow_factory =#  lambda#  cursor, row: row[0]


        # print "Reminder: Want to limit action_taken values when setting up table."

    def add_from_roots(self):
        # Get the list of root directories from the appropriate table. Do a 
        # multithreaded walk along each root, getting all JPG type files and
        # putting them in a list of files to insert. Then call the internal
        # __insert_records_bulk__ function to add all the paths. That function
        # also checks to see if the files are already in the database. 

        get_roots_query = '''SELECT {root_col} FROM {root_tab_name}'''\
        .format(root_tab_name =self.roots_table, root_col =self.roots_col)

        cc = self.conn.cursor()
        cc.execute(get_roots_query)

        roots = cc.fetchall()

        # print roots

        paths = []
        for eachRoot in roots:
            for root, dirs, files in mtw.walk(eachRoot, threads=10):
                for f in files:
                    if f.lower().endswith(tuple(['.jpg', '.jpeg'])):
                        fullPath = os.path.join(root, f)
                        paths.append(fullPath)

        self.__insert_records_bulk__(paths)

    def init_table(self):

        # Set up the database with the appropriate tables and fields. 

        cc = self.conn.cursor()

        # Drop all tables that already exist. This seems to be more robust than
        # using the table names from params for some reason.
        name_query = '''SELECT name FROM sqlite_master WHERE type=\'table\''''
        names = cc.execute(name_query).fetchall()

        for table in names:
            dropTable = '''DROP TABLE IF EXISTS {}'''.format(table)
            cc.execute(dropTable)
            self.conn.commit()

        # Create the photo table, with a full path column, reviewed column,
        # action taken column, and columns for big and small hash values.
        create_table = '''CREATE TABLE {tab_name} (
            {full_path} STRING UNIQUE,
            {reviewed} BOOLEAN NOT NULL CHECK ({reviewed} IN (0, 1) ),
            {action_taken} STRING CHECK ({action_taken} IN ('{action_string}')),
            {big_hash_col} BIGINT,
            {small_hash_col} BIGINT
        );'''.format(tab_name = self.photo_table_name, full_path = self.full_path_col, reviewed=self.reviewed_col,\
             action_taken=self.action_taken_col, big_hash_col = self.big_hash_col, small_hash_col = self.small_hash_col,\
            action_string='\', \''.join(self.poss_actions_list))
        ## CHECK ({action_taken}) IN (0, 1, 2,...)

        cc.execute(create_table)

        # Create the parameter table. I haven't actually used this. 
        create_param_table = '''CREATE TABLE {pt_name} (
            {pm_name} STRING UNIQUE,
            {pm_val}  STRING
        );'''.format(pt_name=self.param_table, pm_name=self.param_name, pm_val=self.param_val)

        cc.execute(create_param_table)

        # Create the table that holds the root directories.
        create_roots_table = '''CREATE TABLE {root_tab_name} ( 
            {root_col} STRING UNIQUE
        );'''.format(root_tab_name =self.roots_table, root_col =self.roots_col)
        cc.execute(create_roots_table)

        # Commit to write all this to disk.
        self.conn.commit()

    def insert_root(self, root_path):

        # Given a root path, add it to the database as a path to look
        # for images. Has checks to ensure that the path is a directory.

        # Make the path into UTF-8 compliant. 
        try:
            utf_path = root_path.encode('utf-8')
        except UnicodeDecodeError as ude:
            utf_path = root_path

        utf_path = u''+utf_path

        # Make the path an absolute path and check that it exists.
        utf_path = os.path.abspath(utf_path)
        if not os.path.isdir(utf_path):
            return False

        # If it's good, go ahead and insert it into the DB.
        insert_query = '''INSERT INTO {root_tab_name} ({root_col}) VALUES (?)'''\
        .format(root_tab_name =self.roots_table, root_col =self.roots_col)

        # Execute the query; this is just wrapped in some try-catch logic.
        try:
            cc = self.conn.cursor()
            cc.execute(insert_query, (utf_path,))
            if self.print_inserts:
                print("Inserted root {}".format(utf_path))
        except sqlite3.IntegrityError as e:
            pass
            e = str(e)
            if 'is not unique' in e or 'constraint failed' in e:
                pass
            else:
                print("Integrity error: {}".format(e))

        # Commit to disk
        self.conn.commit()

    def __insert_records_bulk__(self, full_path_array):

        i = 0
        cc = self.conn.cursor()
        for full_path in full_path_array:
            i += 1
            try:
                utf_path = full_path.encode('utf-8')
            except UnicodeDecodeError as ude:
                utf_path = full_path

            # First, see if this record is in the database:
            check_file_in_db_query = '''SELECT {full_path_col} FROM {photo_table} WHERE {full_path_col} = ?'''\
                .format(photo_table = self.photo_table_name, full_path_col = self.full_path_col)
            
            vals = cc.execute(check_file_in_db_query, (utf_path,)).fetchall() 
            if len(vals) == 0:

                try:
                    minhash, maxhash = self.__hash_img__(utf_path)
                except AttributeError as ae:
                    print(ae + utf_path)
                    continue

                # Try and see if that hash value is already in the table. 
                check_hash_query = '''SELECT {full_path_col} FROM {photo_table} WHERE {big_hash_col} = ? AND {small_hash_col} = ?'''\
                    .format(photo_table = self.photo_table_name, full_path_col = self.full_path_col, \
                        big_hash_col = self.big_hash_col, small_hash_col = self.small_hash_col)
                hvals = cc.execute(check_hash_query, (maxhash, minhash)).fetchall()

                if len(hvals) != 0:
                    # Then we just need to update the file name.
                    previous_path = hvals[0]
                    update_table_query = '''UPDATE {photo_table} SET {full_path_col} = ? WHERE {full_path_col} = ?'''\
                        .format(photo_table = self.photo_table_name, full_path_col = self.full_path_col)
                    cc.execute(update_table_query, (utf_path, previous_path))
                    self.conn.commit()

                else:

                    insert_query = '''INSERT INTO {tab_name} ({full_path_col}, {reviewed_col}, {action_taken_col}, 
                        {big_hash_col}, {small_hash_col}) VALUES (?, 0, '{none_action}', ?, ?)'''\
                        .format(tab_name = self.photo_table_name, full_path_col = self.full_path_col, reviewed_col=self.reviewed_col, \
                            action_taken_col=self.action_taken_col, none_action = self.none_action, \
                            big_hash_col = self.big_hash_col, small_hash_col = self.small_hash_col)

                    try:
                        cc.execute(insert_query, (utf_path, maxhash, minhash))
                        if self.print_inserts:
                            print("Inserted file {}".format(utf_path))
                    except sqlite3.IntegrityError as e:
                        e = str(e)
                        if 'is not unique' in e or 'UNIQUE' in e:
                            pass
                        else:
                            print("Integrity error: {}".format(e))

                if i % 1000 == 0:
                    self.conn.commit()

            else:
                # File is already in database
                pass 

        self.conn.commit()

    def list_unprocessed(self):
        # SQL query to get all the images that do not have the 'reviewed' flag
        # set on them. 
        select_query = '''SELECT {full_path_col} FROM {tab_name} WHERE {reviewed_col} = 0'''\
        .format(tab_name = self.photo_table_name, full_path_col = self.full_path_col, reviewed_col=self.reviewed_col)

        # Get a cursor, fetch all into a list
        c = self.conn.cursor()
        files = c.execute(select_query).fetchall()

        return files

    def list_to_delete(self):
        select_query = '''SELECT {full_path_col} FROM {tab_name} WHERE {reviewed_col} = 1 AND {action_col} = "{del_action}"'''\
        .format(tab_name = self.photo_table_name, full_path_col = self.full_path_col, reviewed_col=self.reviewed_col,
            action_col = self.action_taken_col, del_action = self.poss_actions['action_delete'])

        # Get a cursor, fetch all into a list
        c = self.conn.cursor()
        files = c.execute(select_query).fetchall()

        # files = []

        return files


    def list_all(self):
        select_query = '''SELECT {full_path_col} FROM {tab_name}'''\
        .format(tab_name = self.photo_table_name, full_path_col = self.full_path_col)

        # Get a cursor, fetch all into a list
        c = self.conn.cursor()
        files = c.execute(select_query).fetchall()
        files = [f[0] for f in files]

        return files


    def list_unhashed(self):
        select_query = '''SELECT {full_path_col} FROM {tab_name} WHERE {small_hash_col} IS Null '''\
        .format(tab_name = self.photo_table_name, full_path_col = self.full_path_col, small_hash_col = self.small_hash_col)

        # Get a cursor, fetch all into a list
        c = self.conn.cursor()
        files = c.execute(select_query).fetchall()
        files = [f[0] for f in files]

        return files

    def delete_file(self, filename):
        c = self.conn.cursor()
        if not os.path.exists(filename):
            print("Deleting file {}".format(filename))
            DEL_QUERY =  '''DELETE FROM {tab_name} WHERE {full_path_col} = "{ph_name}"'''\
                    .format(full_path_col = self.full_path_col, tab_name = self.photo_table_name, \
                        ph_name = filename)
            c.execute(DEL_QUERY)
            self.conn.commit()
        else:
            print("File {} is still there!".format(filename))

    def rm_deleted_from_db(self):
        files = self.list_to_delete()

        i = 0

        c = self.conn.cursor()
        for f in files:
            i += 1
            if i % 100 == 0:
                print("{} / {}".format(i, len(files)))
            if not os.path.exists(f):
                DEL_QUERY = '''DELETE FROM {tab_name} WHERE {full_path_col} = "{ph_name}"'''\
                    .format(full_path_col = self.full_path_col, tab_name = self.photo_table_name, \
                        ph_name = f)
                c.execute(DEL_QUERY)

                self.conn.commit()


    def __net_action__(self, init_state, subsequent_action):
        # Function that takes an initial state, a subsequent action,
        # and defines what the new state should be.

        # Define the allowable actions
        none_action = self.poss_actions['action_none']
        clockwise_action = self.poss_actions['action_clockwise']
        ccw_action = self.poss_actions['action_ccw']
        r180_action = self.poss_actions['action_180']
        delete_action = self.poss_actions['action_delete']

        # If the action is none, then we make that 'none'
        if subsequent_action == None:
            subsequent_action = none_action

        # Define a dictionary, so that we can use math rather than
        # a nasty nested if-else tree. 
        action_dict = {none_action:0, 
                       ccw_action:-1,
                       clockwise_action:1,
                       r180_action:2,
                       delete_action:0}

        # New action + old action = new action state. 
        net_action = action_dict[init_state] + action_dict[subsequent_action] 
        
        # Rescale it to 0-3, where we can use a mod to wrap around, then subtract
        # back out. The math works. 
        net_action = (net_action + 1) % 4 - 1

        # Special case - if we marked for deletion, then '0' means delete,
        # not none. 
        if subsequent_action == delete_action:
            out_state = delete_action
        # None means that we can skip the decision tree below. 
        # Allows us to preserve 'deletes' when a 'none' is passed.
        elif subsequent_action == none_action:
            out_state = init_state
        else:
            # Decision tree - decode the actions back to what's defined
            # in the action dict. 
            if net_action == 0:
                out_state = none_action
            elif net_action == 1:
                out_state = clockwise_action
            elif net_action == 2:
                out_state = r180_action
            elif net_action == -1:
                out_state = ccw_action
            else:
                raise ValueError("Action summation does not make sense!")

        return out_state

    def mark_processed(self, full_path, action=None):

        # Encode the path as a UTF-8 path
        try:
            utf_path = full_path.encode('utf-8')
        except UnicodeDecodeError as ude:
            utf_path = full_path

        # Determine if the action is allowable -- otherwise return 'false'.
        if action not in self.poss_actions_list and action is not None:
            return False

        # Get the current action stored in the table -- may be 'none'. 
        get_path_query = '''SELECT {action_col} FROM {photo_table} WHERE {full_path_col} = ?'''\
            .format(photo_table = self.photo_table_name, full_path_col = self.full_path_col, action_col = self.action_taken_col)

        try:
            c = self.conn.cursor()
            currentAction = c.execute(get_path_query, (utf_path,)).fetchall()[0]
        except sqlite3.IntegrityError as e:
            e = str(e)
            print("Integrity error: {}.".format(e, utf_path))

        # Stored action + action taken = new net action
        newStatus = self.__net_action__(currentAction, action)

        # Build a subquery if the action wasn't 'None'. 
        if action is not None:
            action_taken_subquery = ''', {action_taken_col}=\'{action}\' '''.format(action_taken_col = self.action_taken_col, action=newStatus)
        else:
            action_taken_subquery = ''

        # Build the rest of the update query. Makes sure to set the reviewed column to 1. 
        update_query = '''UPDATE {tab_name} SET {reviewed_col}=1 {action_sq} WHERE {full_path_col}= ? '''\
        .format(tab_name = self.photo_table_name, full_path_col = self.full_path_col, reviewed_col=self.reviewed_col, action_sq=action_taken_subquery)

        # Execute. Rely on the CHECK constraint on the table to reject some of the queries. 
        try:
            c = self.conn.cursor()
            c.execute(update_query, (utf_path,))
            if self.print_inserts:
                print("Updated file {}".format(utf_path))
        except sqlite3.IntegrityError as e:
            e = str(e)
            print("Integrity error: {}. File \"{}\" not updated.".format(e, utf_path))

        self.conn.commit()


    def __rotate_cv_img__(self, rotate_command, img_pixels):
        # Helper function to rotate an OpenCV image. Found it on the internet somewhere,
        # but I have tested and know it works. 


        cw_action = self.poss_actions['action_clockwise']
        ccw_action = self.poss_actions['action_ccw']
        r180_action = self.poss_actions['action_180']

        if rotate_command == ccw_action:
            altered_img = cv2.transpose(img_pixels)
            altered_img = cv2.flip(altered_img, flipCode=0)
        elif rotate_command == cw_action:
            altered_img = cv2.transpose(img_pixels)
            altered_img = cv2.flip(altered_img, flipCode=1)
        elif rotate_command == r180_action:
            altered_img = cv2.flip(img_pixels, flipCode=0)
            altered_img = cv2.flip(altered_img, flipCode=1)
        else:
            raise ValueError('Rotate command passed was not valid')
            self.stop_event.set()

        return altered_img

    def __hash_img__(self, filename):
        # Find the hash of the image and all three other rotations.
        # The hash of the image is the minimum of all these hashes.
        # Useful for determining if an image is the same but rotated. 

        # Corner case: '/mnt/server_photos/Pictures_In_Progress/2018/Family Texts/2018-09-15 19.35.19-4.jpeg'

        MINSIZE = 250
        cw_action = self.poss_actions['action_clockwise']
        ccw_action = self.poss_actions['action_ccw']
        r180_action = self.poss_actions['action_180']

        # s = time.time()
        img_pixels = cv2.imread(filename)
        img_pixels = self.crop_img(img_pixels)
        # print("Imread: {}".format(time.time() - s))
        # print filename
        print(img_pixels.shape)
        shapes = img_pixels.shape
        h, w, c = shapes

        # s = time.time()
        top_left = img_pixels[:min(h, MINSIZE), :min(w, MINSIZE), :]
        top_right = img_pixels[:min(h, MINSIZE), -min(w, MINSIZE):, :].copy()
        bottom_left = img_pixels[-min(h, MINSIZE):, :min(w, MINSIZE), :].copy()
        bottom_right = img_pixels[-min(h, MINSIZE):, -min(w, MINSIZE):, :].copy()

        print img_pixels[501:510, 501:510, :]
        # print bottom_left[0, 0, 0]

        # img_ccw = self.__rotate_cv_img__(r180_action, img_pixels)
        # pix_subset = img_ccw[:min(shapes[0], MINSIZE), :min(shapes[1], MINSIZE), :]
        # Top pixels when rotated clockwise
        pixels_cw = self.__rotate_cv_img__(cw_action, bottom_left)
        # Top pixels when rotated counter-clockwise
        pixels_ccw = self.__rotate_cv_img__(ccw_action, top_right)
        # Top pixels, no rotation
        pixels_no_rot = top_left
        # Top pixels when rotated 180 degrees
        pixels_180 = self.__rotate_cv_img__(r180_action, bottom_right)
        # print np.array_equal(pixels_180, pix_subset)
        # print bl_rot[0, 0, 0]
        # print pix_subset[0, 0, 0]

        print pixels_cw[0:5,0,0]
        print pixels_ccw[0:5,0,0]

        hash_orig = hash(pixels_no_rot.tostring())
        hash_cw = hash(pixels_cw.tostring())
        hash_ccw = hash(pixels_ccw.tostring())
        hash_180 = hash(pixels_180.tostring())

        print filename
        print hash_ccw
        print hash_orig
        print hash_cw
        assert( (hash_180 != hash_ccw) or (hash_ccw != hash_cw) or (hash_cw != hash_orig) )

        # print("Hashing: {}".format(time.time() - s))

        # To detect simple rotations, we want the minimum hash. It cuts
        # our hash space somewhat, but collisions are still unlikely. 
        minhash = min(hash_orig, hash_ccw, hash_cw, hash_180)
        maxhash = max(hash_orig, hash_ccw, hash_cw, hash_180)
        # self.img_hash_val = minhash
        return minhash, maxhash

    def crop_img(img_array):
    # Iterative method to remove all-black and all-white bars from all sides
    # of the image, then return the trimmed image. 

        def __crop_color__(color_value, cv_img):
            # Sub-method: remove bars that are all of a given color_value.
            # Written so we don't have to repeat code for black, white, etc. 
            h,w,c = cv_img.shape
            # Define the first and last rows, for the purpose
            # of array access. 
            firstrow = 0
            lastrow = h - 1
            firstcol = 0
            lastcol = w - 1
            # If any of the four corners are of the target color, then it's likely that we will 
            # have bars of that color, and we should iterate. Otherwise, don't bother,
            # because we clearly have one pixel not of that color in each border column
            # and row. 
            if np.all(cv_img[0,0,:] == color_value) or np.all(cv_img[-1, -1,:] == color_value) \
                or np.all(cv_img[0,-1,:] == color_value) or np.all(cv_img[-1, 0,:] == color_value) :
                # These four while loops are similar -- take the respective row or 
                # column, see if all pixels in that vector are of the target color, and
                # while they are, move the crop in one row/column more. 
                while np.all(cv_img[firstrow, :, :] == color_value) and firstrow < h - 1:
                    firstrow += 1
                while np.all(cv_img[lastrow, :, :] == color_value) and lastrow > 0:
                    lastrow -= 1
                while np.all(cv_img[:, firstcol, :] == color_value) and firstcol < w - 1:
                    firstcol += 1
                while np.all(cv_img[:, lastcol, :] == color_value) and lastcol > 0:
                    lastcol -= 1
            # Due to python indexing, we need to add one to the lastrow and lastcol values. 
            lastrow = min(lastrow + 1, h)
            lastcol = min(lastcol + 1, w)
            # A couple of assert statements 
            assert firstcol < lastcol
            assert firstrow < lastrow
            return cv_img[firstrow:lastrow, firstcol:lastcol]
        # Crop black boxes, then white boxes. 
        black_crop = __crop_color__(0, img_array)
        cropped = __crop_color__(255, black_crop)

        return cropped


    def add_hash(self, filename):

        # Check if the file has a hash:

        try:
            utf_path = filename.encode('utf-8')
        except UnicodeDecodeError as ude:
            utf_path = filename


        # Get the current action stored in the table -- may be 'none'. 
        check_hash_query = '''SELECT {big_hash_col}, {small_hash_col} FROM {photo_table} WHERE {full_path_col} = ?'''\
            .format(photo_table = self.photo_table_name, full_path_col = self.full_path_col,\
                big_hash_col = self.big_hash_col, small_hash_col = self.small_hash_col)


        try:
            c = self.conn.cursor()
            currentAction = c.execute(check_hash_query, (utf_path,)).fetchall()[0]
        except sqlite3.IntegrityError as e:
            e = str(e)
            print("Integrity error: {}.".format(e, utf_path))
        
        saved_min = currentAction[0]
        saved_max = currentAction[1]

        if saved_max is None and saved_min is None:
            s = time.time()
            try:
                minhash, maxhash = self.__hash_img__(filename)
            except AttributeError as ae:
                print(ae)
                self.delete_file(filename)
                return
            # print(time.time() - s)

            # Update the hashes
            update_hash_query = '''UPDATE {photo_table} set ({big_hash_col}, {small_hash_col}) = (?, ?) WHERE {full_path_col} = ?'''\
                .format(photo_table = self.photo_table_name, full_path_col = self.full_path_col,\
                    big_hash_col = self.big_hash_col, small_hash_col = self.small_hash_col)

            try:
                c = self.conn.cursor()
                c.execute(update_hash_query, (maxhash, minhash, utf_path,))
            except sqlite3.IntegrityError as e:
                e = str(e)
                print("Integrity error: {}.".format(e, utf_path))

            self.conn.commit()
        # else:
        #     print saved_max