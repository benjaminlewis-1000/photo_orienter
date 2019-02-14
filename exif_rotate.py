#! /usr/bin/env python

import pexif
import sys


def rotate_file(filename, direction):

    img = pexif.JpegFile.fromFile(filename)

    try:
        orientation = img.exif.primary.Orientation[0]
    except AttributeError:
        img.exif.primary.Orientation = [1]
        orientation = 1

    orientation -= 1 # Convert to 0-7

    if direction.lower() == 'left': # Rotate CCW
        if orientation >= 4:
            orientation = orientation ^ 0b010
        orientation = orientation ^ 0b111

    elif direction.lower() == 'right': # Rotate CW
        if orientation < 4:
            orientation = orientation ^ 0b010
        orientation = orientation ^ 0b111
    elif direction.lower() == '180':
        orientation = orientation ^ 0b010

    orientation += 1 # Undo the 0-7
    # print(orientation)
    img.exif.primary.Orientation = [orientation]

    img.writeFile(filename)

if __name__ == "__main__":
    file = sys.argv[1]
    direction = sys.argv[2]

    print(file)
    print(direction)
    rotate_file(file, direction)

'''
Rotate right: (Direction left)
INVERT
1->8  000->111
2->7  001->110
3=>6  010->101
4=>5  011->100
INVERT + Flip 2nd bit (or vice versa)
5=>2  100->001 (011->001) | (110->001)
6=>1  101->000 (010->000) | (111->000)
7=>4  110->011 (001->011) | (100->011)
8=>3  111->010 (000->010) | (101->010)

Rotate "left":
INVERT + Flip 2nd bit
1=>6  000->101  (111->101) | (010->101)
2=>5  001->100  (110->100) | (011->100)
3=>8  010->111  (101->111) | (000->111)
4=>7  011->110  (100->110) | (001->110)
INVERT
5=>4  100->011
6=>3  101->010
7=>2  110->001
8=>1  111->000

Rotate 180:
FLIP 2nd bit  (XOR with 010)
1=>3  000->010
2=>4  001->011
3=>1  010->000
4=>2  011->001
5=>7  100->110
6=>8  101->111
7=>5  110->100
8=>6  111->101
'''
