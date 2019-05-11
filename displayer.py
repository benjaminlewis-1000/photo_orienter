#! /usr/bin/env python2

from pynput.keyboard import Key, Controller
from pynput.keyboard import Listener
import pynput
import threading
import Queue
import cv2
import xmltodict
import os
from exif_rotate import rotate_file
from time import sleep
from collections import deque
import time
import shutil
from populate import tableMinder
import argparse



# A program to view images with functions for rotation and marking for deletion from the dataset.
# Rotation functionality and full deletion functionality are mutually exclusive -- you can only
# launch the program so that it can rotate and mark for deletion, or else it can read the 
# images marked for deletion and allow those to be deleted. 

# The program is launched with 'python2 displayer.py (args)'. Allowable arguments include:
# - None -- the program simply launches for rotation mode
# -d or --delete_photos -- delete photos that were previously marked for deletion
# -c or --cleanup -- read through all the photos marked for deletion and see if they are still 
#       on disk. If they are not on disk, they are removed from the database
# -v or --validate -- validate photos that have been rotated. 

# In viewing mode, there are several commands that are used to navigate the program:
# - 'a' or left arrow -- go back
# - 'd' or right arrow -- advance
# - '9' -- mark for deletion
# '[' -- rotate image counter-clockwise
# ']' -- rotate image clockwise
# '=' or '+' -- rotate image 180 degrees
# 'v' (validate mode only) -- validate that image has been rotated correctly
# 'x' (delete mode only) -- delete image from the file system
# 'r' (delete mode only) -- restore image that was deleted. 
# -:::- Note with deletion and restore -- the backwards queue has a (parameterizable) buffer
# -:::- that is currently set to 20. If you try to go back more than that, it will throw an error,
# -:::- so make sure that you restore images before you advance too much. 
# -:::- Deleting an image will simply move it to the /tmp directory on your computer, so there's still
# -:::- that safety buffer.

# This class is built to load and display images from an instance of populate.py's TableMinder.
# It is multi-threaded. One thread handles displaying images. A parameterizable number of threads
# read images from disk storage, which is ideal if the images are on the network. Another
# thread handles interacting with the database. Communications between threads are handled by 
# Queues. 


# On initialization, the program read

class keyLoggerDisplay():
    """docstring for keyLoggerDisplay"""
    def __init__(self, args):
        self.msg_q = Queue.Queue()
        self.db_q= Queue.Queue()

        self.loaded_imgs_q = Queue.Queue()
        self.next_files_q = Queue.Queue()

        self.max_loaded = 50
        self.num_loaders = 5
        self.max_back_imgs = 20

        self.delete_allowed = False


        script_path  = os.path.abspath(os.path.join(__file__,".."))

        with open(os.path.join(script_path, 'params.xml') ) as stream:
            try:
                self.params = xmltodict.parse(stream.read())
            except Exception as exc:
                print(exc)
                exit(1)

        self.db_class = tableMinder(self.params, print_inserts = False)

        print self.db_class.__hash_img__('/mnt/server_photos/Pictures_In_Progress/preprocess/misc/DSCN4674.JPG')
        print self.db_class.__hash_img__('/mnt/server_photos/Pictures_In_Progress/preprocess/misc/DSCN2154.JPG')
        # exit()

        if args.cleanup:
            self.db_class.rm_deleted_from_db()

        if args.delete_photos:
            self.list_of_files = self.db_class.list_to_delete()
            
            print(len(self.list_of_files))
            self.num_files = len(self.list_of_files)
            self.delete_allowed = True
        elif args.validate:
            print('validate')
            self.list_of_files = self.db_class.list_rotated()
            print(len(self.list_of_files))
            self.num_files = len(self.list_of_files )

        else:
            self.list_of_files = self.db_class.list_unprocessed()

            indb_query = '''SELECT COUNT(*) FROM {}'''.format(self.params['params']['photo_table']['name'])
            total_files = self.db_class.conn.execute(indb_query).fetchall()[0]

            self.num_files = len(self.list_of_files )
            percent_todo = float(self.num_files) / total_files

            print("You are {:.2f}% of the way through with {} files left to do!".format(100 * (1.0 - percent_todo), self.num_files))

        for file in self.list_of_files:
            self.next_files_q.put(file)

        if self.num_files == 0:
            exit()

        self.poss_actions = self.params['params']['possible_actions']

        self.none_action = self.poss_actions['action_none']
        self.clockwise_action = self.poss_actions['action_clockwise']
        self.ccw_action = self.poss_actions['action_ccw']
        self.r180_action = self.poss_actions['action_180']
        self.delete_action = self.poss_actions['action_delete']
        
        self.threads = []

        self.stop_event = threading.Event()

        for algo in [self.threadListen]:
            t = threading.Thread(target=algo, args=(args,))
            self.threads.append(t)
            t.start()

        for i in range(self.num_loaders):
            t = threading.Thread(target=self.__imgLoader__)
            self.threads.append(t)
            t.start()

        self.__dbHandler__()

    def __imgLoader__(self):
        while not self.stop_event.is_set():
            if not self.next_files_q.empty():
                if self.loaded_imgs_q.qsize() < self.max_loaded :
                    next_file = self.next_files_q.get()
                    # print(next_file)
                    if os.path.exists(next_file):
                        try:
                            oriimg = cv2.imread(next_file)

                            height, width, depth = oriimg.shape
                            W = 400.0
                            imgScale = W / width
                            newX,newY = oriimg.shape[1]*imgScale, oriimg.shape[0]*imgScale
                            img = cv2.resize(oriimg,(int(newX),int(newY)))

                            rVal = {'file': next_file, 'image': img}
                            self.loaded_imgs_q.put(rVal)
                        except Exception as e:
                            print("ImgLoader Exception: {}".format(e))
                            print(self.next_files_q.qsize())
                            pass

                        if self.loaded_imgs_q.qsize() < 4:
                            print('Image Queue is of length: {}'.format(self.loaded_imgs_q.qsize()))
                    else:
                        # File no longer exists: it should be removed from database. 
                        thread_db_class = tableMinder(self.params, print_inserts = False)
                        thread_db_class.delete_file(next_file)
            else:
                break

            if self.loaded_imgs_q.qsize() == 0 and self.next_files_q.qsize() == 0:
                print("Setting stop event")
                self.stop_event.set()
                continue
        print("Image loader stopped")

    def __dbHandler__(self):
        while not self.stop_event.is_set():
            if not self.db_q.empty():
                item = self.db_q.get()
                file, action, validate = item
                # print("Processing file {}".format(file))

                if not validate:
                    self.db_class.mark_processed(file, action)
                else:
                    self.db_class.mark_rotation_good(file)
                
                # sleep(0.5)
        print("Database handler stopped")

        
    # Key loggers 
    def on_press(self, key):
        self.msg_q.put(key)

    def on_release(self, key):
        if key == Key.esc or self.stop_event.is_set():
            # Stop listener
            return False

    def keylogger(self):
        # Collect events until released
        with Listener(
                on_press=self.on_press, 
                on_release=self.on_release) as listener:
            listener.join()

    def threadListen(self, args):

        # Checklist - going back, then forward, should load an image
        # as it has been altered - not as it was pushed in the queue. 


        forward_dict_list = deque()
        backward_dict_list = deque()
        backward_files_list = deque()

        def __advanceImage__(current_img_object, validate=False):

            def push_back():
                # A function that only pushes things back if we are 
                # actually progressing to a next image. 
                backward_dict_list.append(current_img_object)
                if len(backward_dict_list) > self.max_back_imgs:
                    back_popped = backward_dict_list.popleft()
                    back_file = back_popped['file']
                    backward_files_list.append(back_file)
                self.db_q.put([current_img_object['file'], None, validate])

            next_loaded = False
            if len(forward_dict_list) == 0:
                if not self.loaded_imgs_q.empty():
                    push_back()
                    current_img_object = self.loaded_imgs_q.get()
                # else:
                #     self.stop_event.set()
            else:
                push_back()
                current_img_object = forward_dict_list.popleft()


            # Put the current file as processed.
            
            # print(current_img_object['file'])
            # print(current_img_object['image'][0][0])
            img = current_img_object['image']
            cv2.imshow('image', img)
            cv2.waitKey(50)

            return current_img_object

        def __rotate_loaded__(img, rotate_command):
            st = time.time()
            if rotate_command == self.ccw_action:
                altered_img = cv2.transpose(img)
                altered_img = cv2.flip(altered_img, flipCode=0)
            elif rotate_command == self.clockwise_action:
                altered_img = cv2.transpose(img)
                altered_img = cv2.flip(altered_img, flipCode=1)
            elif rotate_command == self.r180_action:
                altered_img = cv2.flip(img, flipCode=0)
                altered_img = cv2.flip(altered_img, flipCode=1)
            else:
                raise ValueError('Rotate command passed was not valid')
                self.stop_event.set()
            # print("Took {} seconds to rotate.".format(time.time() - st))
            return altered_img

        while self.loaded_imgs_q.empty():
            if self.stop_event.is_set():
                print("Altering thread stopped")
                return
    
        # Only start the key logger if the stop event isn't already set.
        sleep(0.2)
        if not self.loaded_imgs_q.empty():
            t = threading.Thread(target=self.keylogger)
            self.threads.append(t)
            t.start()
        # sleep(0.01)
            # print(self.stop_event.is_set())

        current_img_object = self.loaded_imgs_q.get()
        img = current_img_object['image']
        filename = current_img_object['file']

        cv2.namedWindow('image', cv2.WINDOW_NORMAL)
        cv2.resizeWindow('image', 600, 600)
        cv2.imshow('image', img)
        cv2.waitKey(500)

        total_delete_size = 0
        while not self.stop_event.is_set():
            # if not self.loaded_imgs_q.empty():
            item = self.msg_q.get()
            try:
                item_char = item.char.lower()
            except AttributeError as ae:
                item_char = ''
                if item == Key.right:
                    item_char = 'd'
                elif item == Key.left:
                    item_char = 'a'
                    
            if item == Key.esc:
                # Stop execution
                del_size_mb = float(total_delete_size) / (1024 ** 2)
                print("Total deleted was {:.2f} MB".format(del_size_mb))
                self.stop_event.set()

            if item_char == 'a':
                # Move BACKWARD
                if len(backward_files_list) == 0 and len(backward_dict_list) == 0:
                    pass # Do nothing -- no files to go back to.
                else:
                    # Tell the database to take care of the current image 
                    # before we change it
                    self.db_q.put([current_img_object['file'], None, False])
                    # Put the current image on the forward list.
                    forward_dict_list.appendleft(current_img_object)
                    try:
                        # Try and get a backward dictionary
                        current_img_object = backward_dict_list.pop()
                        img = current_img_object['image']
                    except IndexError as ie: # Nothing in the backward dict list
                        # Already assured that at least one of the lists has a
                        # length > 0, so backward_files_list should be OK. 
                        filename = backward_files_list.pop()
                        # Read in the image
                        img = cv2.imread(filename)
                        # Form the object. 
                        current_img_object = {'file': filename, 'image': img}
                    cv2.imshow('image', img)
                    cv2.waitKey(50)

            elif item_char == 'd':
                # Push to backward lists
                # print(current_img_object['file'])
                # print(current_img_object['image'][0][0])
                current_img_object = __advanceImage__(current_img_object, validate = False)

            elif item_char == 'v' and args.validate: # Validate
                # Push to backward lists
                current_img_object = __advanceImage__(current_img_object, validate = True)

            elif item_char == 'x':
                fname = current_img_object['file']
                # print(os.path.getsize(fname))
                # total_delete_size += os.path.getsize(fname)
                try:
                    stats = os.stat(fname)
                    filesize = stats.st_size
                    total_delete_size += filesize
                except OSError as ose:
                    pass
                if self.delete_allowed:
                    print("Would delete this file")
                    file_part = fname.split('/')[-1]
                    print(file_part)
                    try:
                        shutil.move(fname, os.path.join('/tmp', file_part) )
                    except IOError as ioe:
                        print("No such file: {}".format(fname))
                    except OSError as ioe:
                        print("No such file: {}".format(fname))
                    current_img_object = __advanceImage__(current_img_object, validate = False)
                else:
                    print("Deletion not allowed in this context")

            elif item_char == 'r':
                fname = current_img_object['file']
                if self.delete_allowed:
                    file_part = fname.split('/')[-1]
                    print(file_part)
                    try:
                        shutil.move(os.path.join('/tmp', file_part), fname)
                    except IOError as ioe:
                        print ("No file to restore -- probably wasn't deleted.")


            elif item_char == 'i':
                # Show information 
                print(current_img_object['file'])

            elif item_char == 't' or item_char == '9':
                # 't' for 'trash' or 'delete'
                self.db_q.put([current_img_object['file'], self.delete_action, False])
                current_img_object = __advanceImage__(current_img_object, validate = False)


            elif item_char == '[':
                # Rotate CCW
                st = time.time()
                filename = current_img_object['file']
                self.db_q.put([filename, self.ccw_action, False])
                rotate_file(filename, 'left')
                img = __rotate_loaded__(current_img_object['image'], self.ccw_action)
                # img = cv2.imread(filename)
                cv2.imshow('image', img)
                cv2.waitKey(50)
                # Update the current image object. 
                current_img_object['image'] = img
                el = time.time() - st
                print("Elapsed: {:02f} sec".format(el))

            elif item_char == ']':
                # Rotate CW
                st = time.time()
                filename = current_img_object['file']
                self.db_q.put([filename, self.clockwise_action, False])
                rotate_file(filename, 'right')
                img = __rotate_loaded__(current_img_object['image'], self.clockwise_action)
                # img = cv2.imread(filename)
                cv2.imshow('image', img)
                cv2.waitKey(50)
                # Update the current image object. 
                current_img_object['image'] = img
                el = time.time() - st
                print("Elapsed: {:02f} sec".format(el))

            elif item_char == '=' or item_char == '+':
                filename = current_img_object['file']
                self.db_q.put([filename, self.r180_action, False])
                # file = self.list_of_files[i % self.num_files]
                rotate_file(filename, '180')
                img = __rotate_loaded__(current_img_object['image'], self.r180_action)
                # img = cv2.imread(filename)
                cv2.imshow('image', img)
                cv2.waitKey(50)
                # Update the current image object. 
                current_img_object['image'] = img

            cv2.waitKey(100)
            # else:
            #     self.stop_event.set()
            #     break

        cv2.destroyWindow('image')
        print("Listener thread stopped")
# imgs = ['tests/byu.jpg', 'tests/byu_left.jpg', 'tests/byu_right.jpg', 'tests/byu_180.jpg']


parser = argparse.ArgumentParser(description="Python to orient photos")
parser.add_argument('-d', '--delete_photos', action='store_true', help='Verify deletions')
parser.add_argument('-c', '--cleanup', action='store_true', help='Clean-up deletions')
parser.add_argument('-v', '--validate', action='store_true', help='Verify rotations')
# parser.add_argument('--root', help='Root in which to search for JPG files')
# parser.add_argument('--update_db', action="store_true", help='Update the database')

args = parser.parse_args()

display = keyLoggerDisplay(args)
