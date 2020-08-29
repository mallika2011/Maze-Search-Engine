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
INV_INDEX_FILE = "index.txt"
answer = {}
posting_list = {}
DEBUG = False

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

def format_query(q):

    title = []
    body = []
    cat = []
    info = []
    ref = []
    links = []
    processed = []
    formatted = [[] for _ in range(7)]
    words = q.split(' ')
    cur ='n'
    fields = ['t:','b:','c:','i:','r:','e:'] 
    for word in words:
        flag = False
        for i,f in enumerate(fields):
            if word[0:2] == f:
                word = word[2:]
                formatted[i+1].append(word)
                cur = f
                flag = True
            elif cur == f:
                formatted[i+1].append(word)
                flag = True
        if not flag:
            formatted[0].append(word)

    for field in formatted:
        field = process_text(' '.join(field))
    
    return formatted


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

    if DEBUG:
        print(doc, i, doc_num, values, sum(values))
    return values,doc_num



'''
Threads per 1e5 rows of the inverted index. 
Each thread searches independently of the other.
'''
def thread_perform_search(chunk, formatted_query):
    
    if DEBUG:
        print("formatted q ", formatted_query)
    
    for line in chunk:

        lis = line.split(':')
        key = lis[0]

        #check if the key in the index belongs to any of the 6 possibilities
        #of the query string. If it does, then obtain the freq of occurance

        for i, q in enumerate(formatted_query):
            if (key in q) :
                docs = lis[1].split('d')
                docs = docs[1:]
                for doc in docs:
                    value,doc_num = get_field_values(doc)

                    if value[i]>0:
                        if doc_num in answer:
                            answer[int(doc_num)]+=int(value[i])
                        else:
                            answer[int(doc_num)]=int(value[i])

                        #add word and doc_num to posting list for phase1
                        if key in posting_list:
                            posting_list[key].append([doc_num,value[i]])
                        else:
                            posting_list[key]=[]
                            posting_list[key].append([doc_num,value[i]])



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

    formatted_query = format_query(q)
    # formatted_query = [words,title,body,cat,info,ref,links]
    
    if DEBUG:
        print("q",q)
        print("w",formatted_query[0])
        print("t",formatted_query[1])
        print("b",formatted_query[2])
        print("c",formatted_query[3])
        print("i",formatted_query[4])
        print("r",formatted_query[5])
        print("l",formatted_query[6])

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
        t = threading.Thread(target=thread_perform_search, args=(CHUNK,formatted_query,))
        threads.append(t)
        t.start()
        k+=CHUNK_SIZE
        j+=CHUNK_SIZE

def write_to_file():

    #write search results into file
    print("writing to file : ", OUTPUT_FILE, "...")

    #Uncomment to view a different formatting.
    # with open('lol.txt','w') as f:
    #     result = "Showing results for \""+str(query_str)+"\"\n\nThis file is read as :-\nDoc_No : Freq\n\n"
    #     for doc in sorted(answer):
    #         result+=str(doc)+" : "+str(answer[doc])+"\n"
    #     f.write(result)

    with open(OUTPUT_FILE,'w') as f:
        result = ""
        for word in posting_list:
            result+="\n=== Word : "+str(word)+" ===\n\n"
            for pairs in posting_list[word]:
                result+="[DocID : "+str(pairs[0])+" - Freq : "+str(pairs[1])+"]\n"

        f.write(result)

if ( __name__ == "__main__"):

    OUTPUT_FILE = 'search/result.txt'
    INV_INDEX_PATH = sys.argv[1]+INV_INDEX_FILE
    query_str = sys.argv[2]
    #create output directory
    output_folder = OUTPUT_FILE.rsplit('/',1)
    create_directory(output_folder[0])

    start_search(query_str)

    #collect all threads
    for t in threads:
        t.join()

    write_to_file()
