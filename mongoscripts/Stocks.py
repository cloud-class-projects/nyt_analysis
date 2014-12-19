import pymongo
import json
import os
# connecting to the mongoclient on the default port
from pymongo import MongoClient
client = MongoClient()

client = MongoClient('localhost', 27017)

# connecting to the DB
db = client.nytimesdb

# Accessing the stock collection
posts = db.stock

directory = '/home/ubuntu/mongoscripts/stockData/'
filenamelist=[]

for root,dirs,files in os.walk(directory,topdown=False):
	for name in files:
		filenamelist.append(name)


#picking one file at a time and opening the file and reading the data and inserting it into the stock collection
for item in filenamelist:
	
	print 'Opening',item
	
	fp = open(directory+item,'r')

	for line in fp.readlines():
	
		json_obj=json.loads(line)
		post_id = posts.insert(json_obj)
        	print 'Inserting Stock data'
	
