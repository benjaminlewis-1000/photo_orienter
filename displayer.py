from pynput.keyboard import Key
from pynput.keyboard import Listener
import threading
import Queue
import cv2
from exif_rotate import rotate_file

imgs = ['byu.jpg', 'byu_left.jpg', 'byu_right.jpg', 'byu_180.jpg']

msg_q = Queue.Queue()
print msg_q

# Key loggers 
def on_press(key):
    msg_q.put(key)

def on_release(key):
    if key == Key.esc:
        # Stop listener
        return False

def keylogger():
    # Collect events until released
    with Listener(
            on_press=on_press, 
            on_release=on_release) as listener:
        listener.join()

def threadListen():
    print("Hi there!")
    img = cv2.imread(imgs[0])
    i = 0
    cv2.namedWindow('image', cv2.WINDOW_NORMAL)
    cv2.resizeWindow('image', 600, 600)
    cv2.imshow('image', img)
    cv2.waitKey(1000)
    while True:
        item = msg_q.get()
        try:
            item_char = item.char
        except AttributeError as ae:
            item_char = ''
            if item == Key.right:
                item_char = 'd'
            elif item == Key.left:
                item_char = 'a'
        # print("Listened to: {}".format(item))
        # print(item)
        # print(type(item))
        # print(dir(item))
        # print(item.char)
        if item == Key.esc:
            break
        if item_char == 'a':
            i -= 1
            img = cv2.imread(imgs[i % 4])
            cv2.imshow('image', img)
        elif item_char == 'd':
            i += 1
            img = cv2.imread(imgs[i % 4])
            cv2.imshow('image', img)
        elif item_char == '[':
            file = imgs[i % 4]
            rotate_file(file, 'left')
            img = cv2.imread(file)
            cv2.imshow('image', img)
        elif item_char == ']':
            file = imgs[i % 4]
            rotate_file(file, 'right')
            img = cv2.imread(file)
            cv2.imshow('image', img)
        elif item_char == '=' or item_char == '+':
            file = imgs[i % 4]
            rotate_file(file, '180')
            img = cv2.imread(file)
            cv2.imshow('image', img)
        cv2.waitKey(50)
        cv2.waitKey(50)



threads = []

for algo in [keylogger, threadListen]:
    t = threading.Thread(target=algo)
    threads.append(t)
    t.start()

print("Hi")