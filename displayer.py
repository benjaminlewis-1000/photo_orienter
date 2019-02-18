from pynput.keyboard import Key
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
from populate import tableMinder


imgs = ['tests/byu.jpg', 'tests/byu_left.jpg', 'tests/byu_right.jpg', 'tests/byu_180.jpg']

# print msg_q

class keyLoggerDisplay():
    """docstring for keyLoggerDisplay"""
    def __init__(self):
        # super(keyLoggerDisplay, self).__init__()
        # self.arg = arg
        self.msg_q = Queue.Queue()
        self.db_q= Queue.Queue()

        self.loaded_imgs_q = Queue.Queue()
        self.next_files_q = Queue.Queue()

        self.max_loaded = 50
        self.num_loaders = 5
        self.max_back_imgs = 20

        script_path  = os.path.abspath(os.path.join(__file__,".."))

        with open(os.path.join(script_path, 'params.xml') ) as stream:
            try:
                self.params = xmltodict.parse(stream.read())
            except Exception as exc:
                print(exc)
                exit(1)

        self.db_class = tableMinder(self.params, print_inserts = False)

        self.list_of_files = self.db_class.list_unprocessed()

        print("You have {} files left to do!".format(len(self.list_of_files)))

        for file in self.list_of_files:
            self.next_files_q.put(file)

        self.num_files = len(self.list_of_files)

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

        for algo in [self.keylogger, self.threadListen]:
            t = threading.Thread(target=algo)
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
                    if os.path.exists(next_file):
                        # print(next_file)
                        try:
                            img = cv2.imread(next_file)

                            rVal = {'file': next_file, 'image': img}
                            self.loaded_imgs_q.put(rVal)
                        except Exception as e:
                            print("Exception: {}".format(e))
                            pass

                        if self.loaded_imgs_q.qsize() < 10:
                            print('Image Queue is of length: {}'.format(self.loaded_imgs_q.qsize()))
        print("Image loader stopped")

    def __dbHandler__(self):
        while not self.stop_event.is_set():
            if not self.db_q.empty():
                item = self.db_q.get()
                file, action = item
                # print("Processing file {}".format(file))

                self.db_class.mark_processed(file, action)
                
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

    def threadListen(self):

        # Checklist - going back, then forward, should load an image
        # as it has been altered - not as it was pushed in the queue. 


        forward_dict_list = deque()
        backward_dict_list = deque()
        backward_files_list = deque()

        def __advanceImage__(current_img_object):

            def push_back():
                # A function that only pushes things back if we are 
                # actually progressing to a next image. 
                backward_dict_list.append(current_img_object)
                if len(backward_dict_list) > self.max_back_imgs:
                    back_popped = backward_dict_list.popleft()
                    back_file = back_popped['file']
                    backward_files_list.append(back_file)
                self.db_q.put([current_img_object['file'], None])

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
            print("Took {} seconds to rotate.".format(time.time() - st))
            return altered_img

        while self.loaded_imgs_q.empty():
            pass

        current_img_object = self.loaded_imgs_q.get()
        img = current_img_object['image']
        filename = current_img_object['file']

        cv2.namedWindow('image', cv2.WINDOW_NORMAL)
        cv2.resizeWindow('image', 600, 600)
        cv2.imshow('image', img)
        cv2.waitKey(500)
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
                self.stop_event.set()

            if item_char == 'a':
                # Move BACKWARD
                if len(backward_files_list) == 0 and len(backward_dict_list) == 0:
                    pass # Do nothing -- no files to go back to.
                else:
                    # Tell the database to take care of the current image 
                    # before we change it
                    self.db_q.put([current_img_object['file'], None])
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
                current_img_object = __advanceImage__(current_img_object)

            elif item_char == 'i':
                # Show information 
                print(current_img_object['file'])

            elif item_char == 't':
                # 't' for 'trash' or 'delete'
                self.db_q.put([current_img_object['file'], self.delete_action])
                current_img_object = __advanceImage__(current_img_object)


            elif item_char == '[':
                # Rotate CCW
                filename = current_img_object['file']
                self.db_q.put([filename, self.ccw_action])
                rotate_file(filename, 'left')
                img = __rotate_loaded__(current_img_object['image'], self.ccw_action)
                # img = cv2.imread(filename)
                cv2.imshow('image', img)
                cv2.waitKey(50)
                # Update the current image object. 
                current_img_object['image'] = img

            elif item_char == ']':
                # Rotate CW
                filename = current_img_object['file']
                self.db_q.put([filename, self.clockwise_action])
                rotate_file(filename, 'right')
                img = __rotate_loaded__(current_img_object['image'], self.clockwise_action)
                # img = cv2.imread(filename)
                cv2.imshow('image', img)
                cv2.waitKey(50)
                # Update the current image object. 
                current_img_object['image'] = img

            elif item_char == '=' or item_char == '+':
                filename = current_img_object['file']
                self.db_q.put([filename, self.r180_action])
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

display = keyLoggerDisplay()
