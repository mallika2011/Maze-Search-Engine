#!/usr/bin/python

import xml.sax
import sys
import os
from os import path, listdir
import nltk
import re
import json
import time
import threading
import psutil

#function to create directories to store results
def create_directory(folder_path):
    my_path = os.getcwd()
    my_path = my_path + '/' +folder_path
    if not os.path.exists(my_path):
        os.makedirs(my_path)
    return my_path

if ( __name__ == "__main__"):

    index_file_path = sys.argv[1]
    output_folder = sys.argv[2]

    create_directory(output_folder)

    with open (index_file_path,'r') as f:

        line = f.readline().strip('\n')

        while(line):
            letter = line[0]

            if not re.match(r"[A-Za-z]",letter):
                letter = "other"

            with open(output_folder+letter+".txt",'a') as f2:
                f2.write(line+'\n')

            line = f.readline().strip('\n')


    