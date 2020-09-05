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
import math

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
INV_INDEX_FILE = "index0.txt"
answer = {}
posting_list = {}
DEBUG = False
FIELD_WEIGHT = 100
K_RESULTS = 50
N = 0

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

    formatted = [[] for _ in range(7)]
    words = q.split(' ')
    cur ='n'
    fields = ['t:','b:','c:','i:','r:','e:'] 
    for word in words:

        #include words in the full count anyways
        formatted[0].append(word)
        for i,f in enumerate(fields):
            if word[0:2] == f:
                word = word[2:]
                formatted[i+1].append(word)
                cur = f
            elif cur == f:
                formatted[i+1].append(word)

    proc_formatted = []
    for field in formatted:
        proc_formatted.append(process_text(' '.join(field)))

    return proc_formatted


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
        

        #if the key belongs to the query ignore this line
        if key not in formatted_query[0]:
            continue
        
        #if the key belongs to the query then extract the 
        #doclist of this word and check the frequency of occurance
        docs = lis[1].split('d')
        docs = docs[1:]

        idf = math.log10( N/len(docs) )

        for doc in docs:
            
            #obtain values for different field queries
            value,doc_num = get_field_values(doc)

            #check whether the word belong to a field query or no
            for i,q in enumerate(formatted_query):
                if key in q and value[i]>0:
                    
                    wt = 1 if i==0 else FIELD_WEIGHT
                    val = wt*(int(value[i]))*idf

                    if doc_num in answer:
                        answer[int(doc_num)]+=val
                    else:
                        answer[int(doc_num)]=val



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
    
    global N
    N = len(Lines)
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

    with open(INDEX_FOLDER+'titles.txt','r') as f1:
        titles = {}
        line = f1.readline().strip('\n')

        while line:
            arr = line.split(':')
            titles[arr[0]] = arr[1]
            line = f1.readline().strip('\n')


    #Uncomment to view a different formatting.
    with open(OUTPUT_FILE,'w') as f:
        result = "Showing top "+str(K_RESULTS)+" results for \""+str(query_str)+"\"\n\nThis file is read as :-\nDoc_ID : Title : Score\n\n"
        
        sort_answers = sorted(answer.items(), key=lambda x: x[1], reverse=True)
        count = 0

        for doc in sort_answers:
            result+=str(doc[0])+"\t:\t"+titles[str(doc[0])]+"\t:\t"+str(doc[1])+"\n"
            count+=1
            if count == K_RESULTS:
                break
        
        #empty search set
        if count == 0:
            result+="Sorry, MAZE was not able to find any results for this query :( ...\n"

        f.write(result)


if ( __name__ == "__main__"):

    OUTPUT_FILE = '2018101041/search/result.txt'
    INDEX_FOLDER = sys.argv[1]
    INV_INDEX_PATH = INDEX_FOLDER+INV_INDEX_FILE
    query_str = sys.argv[2]
    #create output directory
    output_folder = OUTPUT_FILE.rsplit('/',1)
    create_directory(output_folder[0])

    start_search(query_str)

    #collect all threads
    for t in threads:
        t.join()

    write_to_file()
