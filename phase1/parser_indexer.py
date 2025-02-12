#!/usr/bin/python


import xml.sax
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
CHUNK = 1000
stem_words = {}
all_stopwords = stopwords.words('english')
# ss = SnowballStemmer("english")
stemmer = Stemmer.Stemmer('english')
output_folder = ""
stat_path = ""

STAT_FILE = ""
INDEX_FILE_PATH = ""

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
Class handler to manage and parse 
the XML wiki data accordingly.
'''
class WikiHandler(xml.sax.ContentHandler):
    def __init__(self):
        self.CurrentData = ""
        self.data = ""
        self.page_count = 0
        self.all_titles = []
        self.title = ''
        self.text = ''
        self.index = {}
        self.id = ''
        self.id_capture = False

        self.page_titles = []
        self.page_texts = []
        self.page_nos = []

   # Call when an element starts
    def startElement(self, tag, attributes):
        self.CurrentData = tag
        if tag == "page":
            self.data = ''
        
        if tag == "text":
            self.data = ''

        if tag == 'id':
            self.data = ''

    # Call when an elements ends
    def endElement(self, tag):
        if tag == "page":

            self.page_titles.append(self.title)
            self.page_texts.append(self.text)
            self.page_nos.append(self.id)
            self.page_count+=1
            self.id_capture = False

            #create a new thread for every CHUNK pages
            if(self.page_count%CHUNK == 0):
                print("new thread for ", self.page_count, "...")
                t = threading.Thread(target=process_chunk_pages, args=(self.page_titles, self.page_texts, self.page_nos, self.index,self.page_count,))
                threads.append(t)
                t.start()

                #reset 1000 page arrays
                self.page_titles = []
                self.page_texts = []
                self.page_nos = []          
            

        elif tag == "title":
            self.title = self.data
            self.all_titles.append(self.title)
            self.data = ''

        elif tag == "text":
            self.text = self.data
            self.data = ''

        elif tag == 'id':
            if not self.id_capture:
                self.id = self.data
                self.data = ''
                self.id_capture = True

        elif tag == 'mediawiki':
            
            print("new thread for ", self.page_count, "...")
            t = threading.Thread(target=process_chunk_pages, args=(self.page_titles, self.page_texts, self.page_nos, self.index,self.page_count,))
            threads.append(t)
            t.start()

            #reset 1000 page arrays
            self.page_titles = []
            self.page_texts = []
            self.page_nos = []   

            #collect all threads
            for t in threads:
                t.join()

            print("Time to index = ", time.time() - start_time)
            write_to_file(self.index, self.all_titles)
            self.index = {}
            self.all_titles = []
            print("Done")
            print("Total required Time = ", time.time() - start_time)

    # Call when a character is read
    def characters(self, content):
        self.data += content    


'''
Function to process CHUNK sized pages at a time
Each CHUNK will be processed by an individual thread.
'''

def process_chunk_pages(title, text, number, index,num):

    t0 = time.time()
    for i in range(len(title)):
        create_index(title[i],text[i],number[i], index)

    print("Finished processing for ---", num, "in : ", time.time()-t0)

'''
Function to process text for further use
Includes : case folding, tokenization, stop
words removal, and stemming.
'''
def process_text(text,count_tokens=False):
    
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
    if count_tokens:
        global total_tokens
        total_tokens+=len(tokenized_text)


    return(processed)

'''
Function to extract the infobox from the 
pages of the wikipedia dump
'''

def get_infobox(text):
    
    ind = [m.start() for m in re.finditer(r'{{Infobox|{{infobox|{{ Infobox| {{ infobox', text)]
    ans = []
    for i in ind:
        close = False
        counter = 0
        end = -1
        for j in range(i, len(text)-1):
            if text[j]=='}' and text[j+1] =='}':
                counter-=1
            elif text[j]=='{' and text[j+1] =='{':
                counter+=1

            if counter == 0:
                end=j+1
                break
        
        ans+= process_text(text[i:end+1])
    
    return ans

'''
Function to extract the categoris, external links,
and the references from the body of the page and 
process them individually as well.
'''

def split_components(text):
    
    lis = re.split(r"\[\[Category|\[\[ Category", text,1)
    #storing the value for cateogories
    if len(lis)==1:
        category=''
    else:
        category = lis[1]

    lis = re.split(r"==External links==|== External links ==", lis[0],1)
    #storing the value for external links
    if len(lis)==1:
        links = ''
    else:
        links = lis[1]
    
    lis = re.split(r"==References==|== References ==|== references ==|==references==", lis[0],1)
    #storing the value of references
    if len(lis)==1:
        references = ''
    else:
        references = lis[1]

    return category, links, references



'''
Function to create the inverted index
'''
def create_index(title, text, doc_no, index):
    
    c,r,l = split_components(text)

    processed_components = []
    processed_components.append(process_text(title,True))
    try:
        processed_components.append(process_text(text,True))
    except:
        pass
    processed_components.append(process_text(c))
    processed_components.append(get_infobox(text))
    processed_components.append(process_text(r))
    processed_components.append(process_text(l))

    add_to_index(doc_no,processed_components, index)

'''
Function to append an entry to the index object.
'''
def add_to_index(doc_no,processed_components,index):

    for i in range(len(processed_components)):
        processed_tokens = processed_components[i]
        field = i+1

        for token in processed_tokens:

            if(token == ""):
                continue
            freq_values = [0, 0, 0, 0, 0, 0, 0]
            if token not in index:
                freq_values[field] += 1
                freq_values[0] += 1
                index[token] = {}
                index[token][doc_no] = freq_values
            else:
                if doc_no not in index[token]:
                    freq_values[field] += 1
                    freq_values[0] += 1
                    index[token][doc_no] = freq_values
                else:
                    index[token][doc_no][field]+=1
                    index[token][doc_no][0]+=1


def write_to_file(index, titles):

    #write statistics into file
    statistics = str(total_tokens)+"\n"+str(len(index))
    with open(STAT_FILE, "w") as file:
        file.write(statistics)

    #write inverted index into file
    print("writing to file ...")
    ftype = ['f','t', 'b', 'c', 'i', 'r', 'e']
    with open(INDEX_FILE_PATH,'w') as f:
        data = ""
        for key, docs in sorted(index.items()):
            
            #to reduce index size
            if (len(key))>27 or len(index[key])<=1:
                continue

            data += str(key)+":"
            for doc,values in index[key].items():
                data+="d"+str(doc)

                for i in range(len(values)):
                    if values[i]>0:
                        data+=str(ftype[i]) + str(values[i])
            data+="\n"
        f.write(data)

if ( __name__ == "__main__"):

    xml_file = sys.argv[1]
    output_folder = sys.argv[2]
    STAT_FILE = sys.argv[3]

    #create stat directory
    stat_dir = stat_path.rsplit('/',1)

    if len(stat_dir)>1:
        create_directory(stat_dir[0])

    INDEX_FILE_PATH = output_folder+'index.txt'

    # create an XMLReader
    parser = xml.sax.make_parser()
    # turn off namepsaces
    parser.setFeature(xml.sax.handler.feature_namespaces, 0)

    # override the default ContextHandler
    Handler = WikiHandler()
    parser.setContentHandler( Handler )

    parser.parse(xml_file)
