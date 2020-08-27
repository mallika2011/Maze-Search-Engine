#!/usr/bin/python

import sys
import os
import nltk
from nltk import sent_tokenize
from nltk.corpus import stopwords
from nltk.tokenize import RegexpTokenizer
from nltk.tokenize import TreebankWordTokenizer,ToktokTokenizer
from nltk.stem import PorterStemmer 
from nltk.corpus import stopwords
from nltk.stem.snowball import SnowballStemmer
import re
import json
import time
import threading
import Stemmer

# GLOBAL VARIABLES
total_tokens = 0
indexed_tokens = 0
start_time = time.time()
threads = []
end_time = 0
CHUNK_SIZE = 100000
stem_words = {}
all_stopwords = stopwords.words('english')
# ss = SnowballStemmer("english")
stemmer = Stemmer.Stemmer('english')
output_folder = ""
stat_path = ""
ftype = ['f','t','b','c','i','r','e']

STAT_FILE = ""
INDEX_FILE_PATH = ""
INV_INDEX_PATH = './index4/index.txt'
answer = {}

'''
Function to create new directories
'''
#function to create directories to store results
def create_directory(folder_path):
    my_path = os.getcwd()
    my_path = my_path + '/' +folder_path
    if not os.path.exists(my_path):
        os.makedirs(my_path)
    return my_path


def dummy(n):
    print("thread ", n)
    time.sleep(10)
    print("slept 10 for",n)

'''
Split the query words and assign which words
are to be searched in different field query
'''

def process_query(q):
    processed = []
    title = body = cat = info = ref = links = ''
    words = q.split(' ')
    for word in words:
        if(word[0]=='t'):
            title=word
        elif word[0]=='c':
            cat = word
        elif word[0] == 'b':
            body=word
        elif word[0] =='i':
            info=word
        elif word[0] == 'r':
            ref = word
        elif word[0] == 'e':
            links = word
        else:
            processed.append(word)

    processed = process_text(''.join(processed))
    
    return processed, title, body, cat, info, ref, links


'''
Naive method of summing the frequencies of occurances
of the words in their respective fields
'''
def get_field_values(doc):
    
    values = [0]*7
    num = "0"
    ind = 0
    doc_num="cc"

    #remove doc number:
    for i in range(len(doc)):
        if doc[i] in ftype:
            doc_num = doc[:i]
            doc = doc[i:]            
            break

    #count freq
    for char in doc:
        if char not in ftype:
            num+=char
        else:
            values[ind]+=int(num)
            ind = ftype.index(char)
            num="0"
    values[ind]+=int(num)

    print(doc, i, doc_num, values, sum(values))
    return sum(values),doc_num



'''
Threads per 1e5 rows of the inverted index. 
Each thread searches independently of the other.
'''
def thread_perform_search(chunk, words, title, body, cat, info, ref, links):

    for line in chunk:

        lis = line.split(':')
        key = lis[0]
        
        #if key is part of the query string
        if (key in words) or key == title or key == body or key == cat or key == info or key == ref or key == links : 
            docs = lis[1].split('d')
            docs = docs[1:]
            print("came innnnnn", words,key)
            for doc in docs:
                value,doc_num = get_field_values(doc)
                print("vallll ", value, "doccc ", doc_num)
                if doc_num in answer:
                    answer[doc_num]+=int(value)
                else:
                    answer[doc_num]=int(value)
                print("jsoon", answer[doc_num])
        else:
            continue


'''
Function to process query for further use
Includes : case folding, tokenization, stop
words removal, and stemming.
'''
def process_text(text):
    
    processed = []

    #case folding : conver to lower case
    text = text.lower() 

    # tokenize by splitting text
    tokenized_text = re.split(r'[^A-Za-z0-9]+', text)
    tokenized_text = ' '.join(tokenized_text).split()

    #stop words removal
    tokens_without_sw = [token for token in tokenized_text if not token in all_stopwords]

    #stemming : check if the word already exists 
    # in the stem_words set. if does, then use, else stem

    for token in tokens_without_sw:
    
        if token in stem_words:
            stemmed = stem_words[token]
        else:
            # stemmed = ss.stem(token)
            stemmed = stemmer.stemWord(token)
            stem_words[token]=stemmed

        processed.append(stemmed) 
    

    #add to total tokens in the corpus
    global total_tokens
    total_tokens+=len(processed)


    return(processed)


'''
Execute search by creating threads for the
search and retrieval process for the engine
'''
def start_search(q):

    words, title, body, cat, info, ref, links = process_query(q)

    with open(INV_INDEX_PATH, 'r') as f:
        Lines = f.readlines()

    j=0
    k=CHUNK_SIZE
    flag=True

    while(flag):
        if k <=len(Lines):
            CHUNK = Lines[j:k]
        else:
            CHUNK = Lines[j:len(Lines)]
            flag=False

        #thread begins to perform search
        t = threading.Thread(target=thread_perform_search, args=(CHUNK,words, title, body, cat, info, ref, links,))
        threads.append(t)
        t.start()
        k+=CHUNK_SIZE
        j+=CHUNK_SIZE

def write_to_file():

    #write search results into file
    print("writing to file ...")
    with open(OUTPUT_FILE,'w') as f:
        json.dump(answer, f)

if ( __name__ == "__main__"):

    OUTPUT_FILE = sys.argv[1]
    query_str = sys.argv[2]

    #create output directory
    output_folder = OUTPUT_FILE.rsplit('/',1)
    create_directory(output_folder[0])

    start_search(query_str)

    #collect all threads
    for t in threads:
        t.join()

    write_to_file()
