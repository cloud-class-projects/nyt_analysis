import json
from models import articles,parsed_articles # data models for articles and parsed_articles
import os

#directory that has the list of all countries along with the data in json format
directory = '/home/ubuntu/mongoscripts/countries/'

# counter inorder to build a continous set of objects in-order to store the data
change= 0

#name of the object in which data will be stored
obj = 'Article_obj'+ str(change)


#list of countries
#countries = ['India','China','Japan','North Korea','South Korea','Afghanistan',
#'Taiwan','Venezuela','Canada','Australia','Nigeria','Iran','Iraq','Russia',
#'Brazil','Thailand','Cuba','France','Germany','Italy','Israel']

#list of countries
countries = ['China','Japan','Germany']

#lookup to check if value exists in a dicitonary
def lookup(dic, key, *keys):
     if keys:
         return lookup(dic.get(key, {}), *keys)
     return dic.get(key)

'''filebuilder method to walk through the directory of particular country and build 
the filename list needed'''


def filebuilder(item):
    filenamelist=[]
    for root,dirs,files in os.walk(directory+item,topdown=False):
        for name in files:
            filenamelist.append(name)
            
    return filenamelist

            
'''parsing mentod takes the country name and the list of files in the directory of that 
country as arguement and it opens the files inorder to parse the json content'''        

def parsing(fileslist,countryname):
    	default = None
	keywords_dict={}
	keywords_count=[]
	global change
	for fileitem in fileslist: 
        	print fileitem
        	dir_country = countryname + '/'
        	raw_objs_string = open(directory+dir_country+fileitem).read()   #read in raw data
        	raw_objs_string = raw_objs_string.replace('}{', '},{')  	#insert a comma between each object
        	objs_string = '[%s]'%(raw_objs_string)             		#wrap in a list, to make valid json
        	objs = json.loads(objs_string) 					#json to bring it into python land

        	for item in objs:						#Extraction of entities from the JSON
            		for x in item['response']['docs']:			
                		obj = articles()				#creating the article object
				obj.country = countryname 			#Storing the country information
                		obj.type_of_material = x['type_of_material']	#Type of material of news article
                		obj.news_desk = x['news_desk']			#News_desk
                		obj.lead_paragraph = x['lead_paragraph']	#Lead paragraph of the article
                		if x['abstract'] is None:			# Article abstract and check if its exists 
                    			obj.abstract = 'Empty'              
                		else:
                    			obj.abstract = x['abstract']
                    		obj._id= x['_id']				#Article ID 
                    		obj.snippet = x['snippet']			#Article snippet
                    		obj.web_url = x['web_url']			#Article Web URL
				
				#Splitting of the publication date inorded to extract year,month and day 
                    		
				test = x['pub_date'].split('-')
                    		obj.pub_year = test[0]
                    		obj.pub_month = test[1]
				day = test[2]
				obj.pub_day = day[:2]
				obj.source = x['source']			#Article source
                   		obj.section_name = x['section_name']		#Article section
                    		obj.subsection_name = x['subsection_name']	#Article subsection
            
                    		dict_headline= x.get('headline',default)	#Article headline
				
				#if headline main exists use it else pick default which is None 
			
				if dict_headline: 				
                    			main_part  = lookup(dict_headline,'main') 
                    			obj.headline = main_part
				else:
					obj.headline = default
                    		obj.document_type = x['document_type']		#Article document_type like article,blog,reviews
                    		obj.word_count = x['word_count']
               
                    		for z in x['keywords']:				#Extracting keywords
        								
                 			if z.get('rank',default):		#Extracting the rank from keywords		
                            			keywords_dict['Rank'] = z['rank']
            
                        		if z.get('is_major',default):		#is_major field extraction
                            			keywords_dict['is_major'] = z['is_major']            
                        		if z.get('name',default):
                            			keywords_dict['Name'] = z['name'] # Name and value extraction
                        		if z.get('value',default):
                            			keywords_dict['Value'] = z['value']

                            		keywords_count.append(keywords_dict)
                            		keywords_dict={}
                    
                    		obj.keywords = keywords_count    
                    		keywords_count = []    
                    
                    		print ' Saving the current object'
                    		print 'Current object ID is',x['_id']
        
                    		obj.save()					#Saving the object to mongoDB
        			change = change+1				#updating the counter for new object
            			obj = 'Article_obj'+ str(change)		#building the new object 
        


def main():
    
    #picks up a country from the countries list calls filebuilder with country name as arguement
    for countryname in countries:
        fileslist = filebuilder(countryname)
        print '>>>>>>>>>>>>>>>>>>>>', countryname
	#call to parsing to parse the countryname and the corresponding list of files obtained
	parsing(fileslist,countryname)


if __name__ == '__main__':main()
