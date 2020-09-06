#!/usr/bin/python


import xml.sax
import sys
import os
from os import path, listdir
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
import psutil

# GLOBAL VARIABLES
total_tokens = 0
indexed_tokens = 0
start_time = time.time()
CHUNK = 10000
stem_words = {}
all_stopwords = stopwords.words('english')
stemmer = Stemmer.Stemmer('english')
output_folder = ""
stat_path = ""

STAT_FILE = ""
INDEX_FILE_PATH = ""
TOTAL_INDICES = 0


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
        self.all_titles = {}
        self.title = ''
        self.text = ''
        self.id = ''
        self.id_capture = False

        self.page_titles = []
        self.page_texts = []
        self.page_ids = []

   # Call when an element starts
    def startElement(self, tag, attributes):
        self.data = ''

    # Call when an elements ends
    def endElement(self, tag):
        global TOTAL_INDICES

        if tag == "page":

            self.page_titles.append(self.title)
            self.page_texts.append(self.text)
            self.page_ids.append(self.id)
            self.page_count+=1
            self.id_capture = False

            self.all_titles[self.id] = self.title

            #create a new thread for every CHUNK pages
            if(self.page_count%CHUNK == 0):

                process_chunk_pages(self.page_titles, self.page_texts, self.page_ids,self.all_titles)

                #reset page arrays
                self.page_titles.clear()
                self.page_texts.clear()
                self.page_ids.clear()
                self.all_titles={}
                TOTAL_INDICES +=1 

        elif tag == "title":
            self.title = self.data
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
            
            process_chunk_pages(self.page_titles, self.page_texts, self.page_ids,self.all_titles)

            #reset page arrays
            self.page_titles.clear()
            self.page_texts.clear()
            self.page_ids.clear()     
            self.all_titles = {}
            TOTAL_INDICES+=1 
                        

    # Call when a character is read
    def characters(self, content):
        self.data += content    


'''
Function to process CHUNK sized pages at a time
Each CHUNK will be processed by an individual thread.
'''

def process_chunk_pages(title, text, ids,all_titles):

    index = {}
    for i in range(len(title)):
        create_index(title[i],text[i],ids[i], index)

    #write all the titles to the file
    with open(output_folder+'titles.txt','a') as t:
        
        my_titles = ""
        for doc_id, title in all_titles.items():
            my_titles+=str(doc_id).strip()+":"+str(title).strip()+"\n"

        t.write(my_titles)

    write_to_file(index, title)


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
    # statistics = str(total_tokens)+"\n"+str(len(index))
    # with open('./index/index'+str(INDEX_FILE_PATH)+'.txt', "w") as file:
    #     file.write(statistics)

    #write inverted index into file
    ftype = ['f','t', 'b', 'c', 'i', 'r', 'e']
    with open(INDEX_FILE_PATH+"index"+str(TOTAL_INDICES)+'.txt','w') as f:
        data = ""
        for key, docs in sorted(index.items()):
            
            #to reduce index size
            if (len(key))>27:
                continue
            
            data += str(key)+":"
            for doc,values in index[key].items():
                data+="d"+str(doc)

                for i in range(len(values)):
                    if values[i]>0:
                        data+=str(ftype[i]) + str(values[i])
            data+="\n"
        f.write(data)


def merge(file1, file2,n1,n2):  

    merge_file = output_folder + "merge.txt"

    with open(merge_file,'w') as m, open(file1,'r') as f1, open(file2,'r') as f2:
        
        line1 = f1.readline().strip('\n')
        line2 = f2.readline().strip('\n')

        while line1 or line2:
            
            if not line1:
                m.write(line2+'\n')
                line2 = f2.readline().strip('\n')
            elif not line2:
                m.write(line1+'\n')
                line1 = f1.readline().strip('\n')

            else:

                w1 = line1.split(':')[0]
                w2 = line2.split(':')[0]
                
                if w1<w2:  
                    m.write(line1+'\n')  
                    line1 = f1.readline().strip('\n')
                elif w2<w1:  
                    m.write(line2+'\n')  
                    line2 = f2.readline().strip('\n')

                elif w1 == w2:
                    l = w1 +':'+line1.strip().split(":")[1]+line2.strip().split(':')[1] + '\n'
                    m.write(l)
                    line1 = f1.readline().strip('\n')
                    line2 = f2.readline().strip('\n')


        renamed_file = output_folder+'index'+str(int(n1/2))+'.txt'

        os.remove(file1)
        os.remove(file2)
        os.rename(merge_file, renamed_file)

def merge_sort(pages):  
      
    while pages != 1:

        for i in range(0, pages, 2):
            if i + 1 == pages:
                old_name = output_folder+"index"+str(i)+'.txt'
                new_name = output_folder+"index"+ str(int(i/2))+'.txt'
                os.rename(old_name, new_name)
                break
        
            file1 = output_folder+"index"+str(i)+'.txt'
            file2 = output_folder+"index"+str(i+1)+'.txt'
            merge(file1, file2,i,i+1)

        if pages % 2 == 1:
            pages = pages // 2 + 1
        else:
            pages = pages // 2



def get_file_list(mypath):
    onlyfiles = [path.join(mypath, f) for f in listdir(mypath) if path.isfile(path.join(mypath, f))]
    return onlyfiles,len(onlyfiles)

if ( __name__ == "__main__"):

    xml_file_path = sys.argv[1]
    output_folder = sys.argv[2]
    STAT_FILE = sys.argv[3]
    INDEX_FILE_PATH = output_folder
    all_xml_files,n= get_file_list(xml_file_path)

    for i,file in enumerate(all_xml_files) :

        start = time.time()

        # create an XMLReader
        parser = xml.sax.make_parser()
        # turn off namepsaces
        parser.setFeature(xml.sax.handler.feature_namespaces, 0)

        # override the default ContextHandler
        Handler = WikiHandler()
        parser.setContentHandler( Handler )

        print("========= Parsing file ",str(i), "with toatal indices ", TOTAL_INDICES," ===========")
        print("Memory used = ",psutil.virtual_memory().percent,'\n\n')

        parser.parse(file)

        print("Mini index created in = ", time.time()-start)
    
    print("Total mini indices = ",TOTAL_INDICES)
    merge_sort(TOTAL_INDICES) 

    print("Total required Time = ", time.time() - start_time)