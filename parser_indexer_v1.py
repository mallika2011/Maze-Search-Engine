#!/usr/bin/python


import xml.sax
import sys
import os
import nltk
# nltk.download('punkt')
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

# GLOBAL VARIABLES
total_tokens = 0
indexed_tokens = 0
start_time = time.time()
threads = []
end_time = 0
CHUNK = 1000
stem_words = {}
all_stopwords = stopwords.words('english')
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
    print("slept 5 for",n)

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

        self.page_titles = []
        self.page_texts = []
        self.page_nos = []

   # Call when an element starts
    def startElement(self, tag, attributes):
        self.CurrentData = tag
        if tag == "page":
            self.data = ''

    # Call when an elements ends
    def endElement(self, tag):
        if tag == "page":
            
            self.page_titles.append(self.title)
            self.page_texts.append(self.text)
            self.page_nos.append(self.page_count)
            self.page_count+=1

            #create a new thread for every CHUNK pages
            if(self.page_count%CHUNK == 0):
                print("new thread for ", self.page_count, "...")
                # t = threading.Thread(target=create_index, args=(self.title, self.text, self.page_count, self.index,))
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

        elif tag == 'mediawiki':

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
Function to process text for further use
Includes : case folding, tokenization, stop
words removal, and stemming.
'''

def process_chunk_pages(title, text, number, index,num):

    t0 = time.time()
    for i in range(len(title)):
        create_index(title[i],text[i],number[i], index)

    print("Finished processing for ---", num, "at : ", time.time()-t0)

def process_text(text):
    
    processed = []

    #case folding : conver to lower case
    text = text.lower() 

    #tokenize text

    #using another alternative
    # tokenized_text = re.sub(r'[^A-Za-z0-9]+', r' ', text.encode("ascii", errors="ignore").decode())

    #using toktok tokenizer
    # toktok = ToktokTokenizer()
    # # tokenized_text = [toktok.tokenize(sent) for sent in sent_tokenize(text)]
    # tokenized_text = toktok.tokenize(text)

    # print(tokenized_text)
    
    #using treebank tokenizer
    # tokenizer = TreebankWordTokenizer()
    #using the regex tokenizer
    # tokenizer = RegexpTokenizer('[a-zA-Z]\w+\'?\w*')
    # tokenized_text = tokenizer.tokenize(text)
    
    # tokenize by splitting text
    tokenized_text = re.split(r'[^A-Za-z0-9]+', text)
    # tokenized_text = re.findall("[A-Z]{2,}(?![a-z])|[\w]+", str(text))

    if tokenized_text[0]=="":
        del tokenized_text[0]

    if tokenized_text[-1]=="":
        del tokenized_text[-1]

    #stop words removal
    tokens_without_sw = [token for token in tokenized_text if not token in all_stopwords]

    #stemming : check if the word already exists 
    # in the stem_words set. if does, then use, else stem
    
    # ps = PorterStemmer()

    ss = SnowballStemmer("english")
    for token in tokens_without_sw:
    
        if token in stem_words:
            stemmed = stem_words[token]
        else:
            stemmed = ss.stem(token)
            stem_words[token]=stemmed

        processed.append(stemmed) 
    

    #add to total tokens in the corpus
    global total_tokens
    total_tokens+=len(processed)

    return(processed)


def get_category(text):
    lists = re.findall(r"\[\[Category:(.*)\]\]", str(text))
    ans = []
    for curr in lists:
        temp = process_text(curr)
        ans += temp
    
    return ans

def get_infobox(text):
    raw = text.split("{{Infobox")
    ans = []
    if len(raw) <= 1:
        return []
    for ind in range(1,len(raw)):
        traw = raw[ind].split("\n")
        for lines in traw:
            if lines == "}}":
                break
            ans += process_text(lines)
    return ans

def get_externallinks(text):
    raw = text.split("== External links ==")
    ans = []

    if len(raw) <= 1:
        return []
    raw = raw[1].split("\n")
    for lines in raw:
        if lines and lines[0] == '*':
            line = process_text(lines)
            ans += line
    return ans 

def get_references(text):
    raw = text.split("== References ==")
    ans = []
    if len(raw) <= 1:
        return []
    raw = raw[1].split("\n")
    for lines in raw:
        if ("[[Category" in lines) or ("==" in lines) or ("DEFAULTSORT" in lines):
            break
        line = process_text(lines)
        if "Reflist" in line:
            line.remove("Reflist")
        if "reflist" in line:
            line.remove("reflist")
        ans += line
    return ans


'''
Function to create the inverted index
'''
def create_index(title, text, doc_no, index):
    title_tok = process_text(title)
    add_to_index(doc_no,title_tok,1, index)

    # Body (Index : 1)
    try:
        add_to_index(doc_no, process_text(text),2, index)
    except:
        pass

    # Category (Index : 2)
    add_to_index(doc_no, get_category(text),3, index)

    # Infobox (Index : 3)
    add_to_index(doc_no, get_infobox(text),4, index)

    # References (Index : 4)
    add_to_index(doc_no, get_references(text),5, index)

    # External Links (Index : 5)
    add_to_index(doc_no, get_externallinks(text),6, index)
        

'''
Function to append an entry to the index object.
'''
def add_to_index(doc_no,processed_tokens, field, index):

    # print(processed_tokens,"field = ", field)

    for token in processed_tokens:
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
    file = open(INDEX_FILE_PATH, 'w')
    for ind, word in enumerate(sorted(index.keys())):
        mystr = word + ':'
        for doc in index[word]:
            freq = index[word][doc]
            mystr += "d" + str(doc)
            for ind, fs in enumerate(freq):
                if fs > 0:
                    mystr += str(ftype[ind]) + str(fs)
        file.write(mystr + "\n")
    file.close()

    # with open("./index/index.txt", "w") as file:
    #     json.dump(index, file)

if ( __name__ == "__main__"):

    xml_file = sys.argv[1]
    output_folder = sys.argv[2]
    STAT_FILE = sys.argv[3]

    #create stat directory
    stat_dir = stat_path.rsplit('/',1)

    if len(stat_dir)>1:
        create_directory(stat_dir[0])

    INDEX_FILE_PATH = output_folder+'/index.txt'

    # create an XMLReader
    parser = xml.sax.make_parser()
    # turn off namepsaces
    parser.setFeature(xml.sax.handler.feature_namespaces, 0)

    # override the default ContextHandler
    Handler = WikiHandler()
    parser.setContentHandler( Handler )

    parser.parse(xml_file)
