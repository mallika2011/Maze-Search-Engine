<script src="//yihui.org/js/math-code.js"></script>
<!-- Just one possible MathJax CDN below. You may use others. -->
<script async
  src="//mathjax.rstudio.com/latest/MathJax.js?config=TeX-MML-AM_CHTML">
</script>

# MAZE Search Engine

The MAZE search engine is a search tool built on a standard dump of Wikipedia atricles. The corpus is approximately 40GB in size. 
The search engine supports two kinds of queries - ***normal queries*** and ***field queries*** 

## Setup and Execution

1. Clone the repository

```bash
git clone https://github.com/mallika2011/Search-Engine.git
```

2. Create a venv and install requirements

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

3. Run the **indexer** code on the wiki dump. It takes 3 arguments. <br/>The second argument is the path to the folder where the index will be created. And the third argument is a text file that will store statistical details about the dump after and before indexing.

```bash
./index.sh <path to wiki dump XML folder> <path to inverted index output folder> <path to statistics .txt file>
```

4. Run the **splitting** code. <br/> This will split the index into multiple smaller sized index files, for better modularity.

```bash
python3 split.py
```

5. Run the **search** script 

```
python3 search.py <path the queries.txt file>
```

## Indexing Technique 

In order to index the 40 GB corpus of data, the following steps are carried out : 

*  **Parsing and Chunkwise blocking:**<br/>
The XML parser has been used to parse the XML files containing the wiki dump. For every ***K*** pages read by the parser (here pages imply wiki articles), the text of the article, the title and the IDs of these ***K*** pages are stored in memory. After K pages are read, the data in memory is then processed and written to a mini index file.

* **Splitting Components**<br/>
In order to support field queries, the different components of the Wiki Articles are split using *regex* and text matching techniques.

*  **Text Processing**<br/>
All the text is proccessed by case folding (lowercase), tokenisation, stop words removal and stemming. In order to improve the stemmign time, a JSON is maintained to keep track of the stemmed words encountered so far. This reduces the stemming time as some stem words are now found in *```O(1)```*.

*  **Merging mini index files** <br/>
Once the entire dump has been parsed a number of mini index files will be created (in this case 999 mini index files). These are then merged into a single ***index0.txt*** file by the conventional merge sort technique. 

*  **Splitting merged index alphabetically** <br/>
The merged index is finally broken down into smaller index files again, this time ensuring that the smaller files are for each alphabet and one for all other characters. Hence there are a total of ***27 index files***, finally.


*  **Convention followed in the index** <br/>
Each posting list has a list of documents and the associated frequencies for the different fields in the wiki article. An example of how the posting lists look like is  : *```schoolbag:d40187f2b1r1d55615f1b1d13913f1b1d16027f1b1d22648f2b2```*

## Searching Mechanism

The search script produces results for top K documents. Every query is of the following format : *"K, <query_text>"* where query text could include standard or field queries. <br/>Eg of a field query : *"5, t:sachin"*

The query text is first ***processed*** via the same text processing as in the index creation. The tokens are then ***segregated*** to split the field queries (if any). The posting lists of the words in the query are then retrieved from the corresponding letters' index files. 

Once the posting lists have been retrieved, each document in the posting list is assigned a score. This socre is computed via the ***Tf-IDf principle***. The formula for the weight of a word ***i*** in a document ***j*** ie: ***w<sub>i,j</sub>*** is : 

***w<sub>i,j</sub> = tf<sub>i,j</sub> • log<sub>10</sub>(N/di)***

Further, to improve the relevancy of the search instead of using simply ***tf<sub>i,j</sub>*** as the term frequency, ***1+ log<sub>10</sub>(tf<sub>i,j)</sub>*** is used. 

This is to prevent accounting for any kind of term spamming and also to ensure uniformity as the second occurence of a word does not weigh more than the first.
