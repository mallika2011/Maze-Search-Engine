#starter script to run indexer

#clean and create output directory
rm -rf $2
mkdir $2

python3 2018101041/parser_indexer.py $1 $2 $3


