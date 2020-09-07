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
posting_list = {}
DEBUG = False
FIELD_WEIGHT = [1,500,100,200,200,100,100]
K_RESULTS = 50
TOTAL_TIME = 0

'''
Function to create new directories
'''
#function to create directories to store results
def create_directory(folder_path):
    my_path = os.getcwd()
    my_path = my_path + '/' +folder_path
    if not os.path.exists(my_path):
        os.makedirs(my_path)
    elif os.path.isfile(my_path+"/queries_op.txt"):
        # print(my_path+"/queries_op.txt")
        os.remove(my_path+"/queries_op.txt")
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
def perform_search(file, formatted_query):
    
    if DEBUG:
        print("formatted q ", formatted_query)
    
    with open(file, 'r') as f1:

        line = f1.readline().strip('\n')

        while line:

            # print(line)
            lis = line.split(':')
            key = lis[0]
            

            #if the key belongs to the query ignore this line
            if key not in formatted_query[0]:
                line = f1.readline().strip('\n')
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
                        
                        wt = FIELD_WEIGHT[i]
                        # val = wt*(int(value[i]))*idf

                        val = wt*(1 + math.log10(int(value[i])))*idf


                        if int(doc_num) in answer:
                            answer[int(doc_num)]+=val
                        else:
                            answer[int(doc_num)]=val

            
            line = f1.readline().strip('\n')



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


def get_letters(fq):

    letters = []
    for part in fq:
        for word in part :
            if re.match(r"[A-Za-z]",word[0]):
                letters.append(word[0])
            else:
                letters.append("other")
    return letters

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


    letters = get_letters(formatted_query)

    for x in letters :
        file = INDEX_FOLDER + "split/" + x + ".txt"
        # perform_search(file,formatted_query,)
        #thread begins to perform search
        t = threading.Thread(target=perform_search, args=(file,formatted_query,))
        threads.append(t)
        t.start()


def get_N(fname):
    with open(fname) as f:
        for i, l in enumerate(f):
                pass
    return i + 1


def get_titles():
    with open(INDEX_FOLDER+'titles.txt','r') as f1:
        line = f1.readline().strip('\n')
        count = 0
        while line:
            arr = line.split(':')
            if int(arr[0]) in answer:
                titles[arr[0]] = arr[1]
                count+=1
            elif len(answer) < K_RESULTS and count < K_RESULTS:
                sec_titles[arr[0]] = arr[1]
                count+=1
            
            line = f1.readline().strip('\n')

def write_to_file(timetaken):

    #write search results into file
    with open(OUTPUT_FILE,'a') as f:
        result = "Showing top "+str(K_RESULTS)+" results for \""+str(query_str)+"\"\n\nThis file is read as :-\nDoc_ID : Score : Title\n\n"
        
        sort_answers = sorted(answer.items(), key=lambda x: x[1], reverse=True)
        count = 0

        for doc in sort_answers:
            result+=str(doc[0])+"\t:\t"+str(doc[1])+"\t:\t"+titles[str(doc[0])]+"\n"
            count+=1
            if count == K_RESULTS:
                break
        
        #in case answers aren't sufficient to match K results
        for doc in sec_titles:

            if count == K_RESULTS:
                break
            result+=str(doc)+"\t:\t"+str(0.000000)+"\t:\t"+sec_titles[str(doc)]+"\n"
            count+=1
        
        #empty search set
        if count == 0:
            result+="Sorry, MAZE was not able to find any results for this query :( ...\n"

        result+="\n Time Taken :  Total = "+str(timetaken) +" Avg = "+str(timetaken/K_RESULTS)+"\n"
        result+="=====================================================================================\n"
        f.write(result)


if ( __name__ == "__main__"):

    print("\n===========================================")
    print("       MAZE SEARCH ENGINE : ACTIVATED      ")
    print("===========================================\n\n")

    OUTPUT_FILE = 'search/queries_op.txt'
    INDEX_FOLDER = sys.argv[1]
    INV_INDEX_PATH = INDEX_FOLDER+INV_INDEX_FILE
    # query_str = sys.argv[2]

    query_file = sys.argv[2]

    #create output directory
    output_folder = OUTPUT_FILE.rsplit('/',1)
    create_directory(output_folder[0])

    #clear output file
    # os.remove(OUTPUT_FILE)

    q_num = 0
    N = get_N(INDEX_FOLDER+'titles.txt')

    print("Started Search ... ")
    with open(query_file,'r') as qf:

        line = qf.readline().strip('\n')

        while(line):
            answer = {}
            sec_titles = {}
            titles = {}
            line = line.split(',')

            q_num+=1
            K_RESULTS = int(line[0])
            query_str = line[1].strip()

            s = time.time()
            #start searching for each query
            start_search(query_str)

            #collect all threads
            for t in threads:
                t.join()

            print("Search Done. Writing to File ...")
            time_taken = time.time()-s
            get_titles()
            write_to_file(time_taken)
            

            TOTAL_TIME += time_taken
            
            line = qf.readline().strip('\n')

    print("\n===========================================\n")
    print("Total docs = ", N)
    print("Total time for", q_num,"queries = ", TOTAL_TIME)
    print("Average time for", q_num,"queries = ", TOTAL_TIME/q_num)
    print()
    print("Results written to file : ", OUTPUT_FILE, "...")