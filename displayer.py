from pynput.keyboard import Key
from pynput.keyboard import Listener
import threading
import Queue
import cv2
import xmltodict
import os
from exif_rotate import rotate_file
from time import sleep
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


        script_path  = os.path.abspath(os.path.join(__file__,".."))

        with open(os.path.join(script_path, 'params.xml') ) as stream:
            try:
                self.params = xmltodict.parse(stream.read())
            except Exception as exc:
                print(exc)
                exit(1)

        self.db_class = tableMinder(self.params, print_inserts = False)

        self.list_of_files = self.db_class.list_unprocessed()

        print(len(self.list_of_files))
        self.num_files = len(self.list_of_files)


        self.poss_actions = self.params['params']['possible_actions']

        self.none_action = self.poss_actions['action_none']
        self.clockwise_action = self.poss_actions['action_clockwise']
        self.ccw_action = self.poss_actions['action_ccw']
        self.r180_action = self.poss_actions['action_180']
        self.delete_action = self.poss_actions['action_delete']

        # exit()
        
        self.threads = []

        for algo in [self.keylogger, self.threadListen]:
            t = threading.Thread(target=algo)
            self.threads.append(t)
            t.start()

        self.__dbHandler__()


    def __dbHandler__(self):
        while True:
            item = self.db_q.get()
            file, action = item
            if file == 'EXIT':
                break

            self.db_class.mark_processed(file, action)
            
            sleep(0.5)

        
    # Key loggers 
    def on_press(self, key):
        self.msg_q.put(key)

    def on_release(self, key):
        if key == Key.esc:
            # Stop listener
            return False

    def keylogger(self):
        # Collect events until released
        with Listener(
                on_press=self.on_press, 
                on_release=self.on_release) as listener:
            listener.join()

    def threadListen(self):
        print("Hi there!")
        img = cv2.imread(self.list_of_files[0])
        i = 0
        cv2.namedWindow('image', cv2.WINDOW_NORMAL)
        cv2.resizeWindow('image', 600, 600)
        cv2.imshow('image', img)
        cv2.waitKey(1000)
        while True:
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
                self.db_q.put(['EXIT', 'EXIT'])
                break
            if item_char == 'a':
                self.db_q.put([self.list_of_files[i % self.num_files], None])
                i -= 1
                img = cv2.imread(self.list_of_files[i % self.num_files])
                cv2.imshow('image', img)
            elif item_char == 'd':
                self.db_q.put([self.list_of_files[i % self.num_files], None])
                i += 1
                img = cv2.imread(self.list_of_files[i % self.num_files])
                cv2.imshow('image', img)
            elif item_char == '[':
                # Rotate CCW

        # self.none_action = self.poss_actions['action_none']
        # self.clockwise_action = self.poss_actions['action_clockwise']
        # self.ccw_action = self.poss_actions['action_ccw']
        # self.r180_action = self.poss_actions['action_180']
        # self.delete_action = self.poss_actions['action_delete']

                self.db_q.put([self.list_of_files[i % self.num_files], self.ccw_action])
                file = self.list_of_files[i % self.num_files]
                rotate_file(file, 'left')
                img = cv2.imread(file)
                cv2.imshow('image', img)
            elif item_char == ']':
                self.db_q.put([self.list_of_files[i % self.num_files], self.clockwise_action])
                file = self.list_of_files[i % self.num_files]
                rotate_file(file, 'right')
                img = cv2.imread(file)
                cv2.imshow('image', img)
            elif item_char == 'i':
                print(self.list_of_files[i % self.num_files])
            elif item_char == 'd':
                self.db_q.put([self.list_of_files[i % self.num_files], self.delete_action])
            elif item_char == '=' or item_char == '+':
                self.db_q.put([self.list_of_files[i % self.num_files], self.r180_action])
                file = self.list_of_files[i % self.num_files]
                rotate_file(file, '180')
                img = cv2.imread(file)
                cv2.imshow('image', img)
            cv2.waitKey(50)
            cv2.waitKey(50)


display = keyLoggerDisplay()
