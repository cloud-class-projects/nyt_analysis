import json
from models_companies import articles,parsed_articles # company data models
import os


#directory containing companies related news articles
directory = '/home/ubuntu/mongoscripts/companies/'

change= 0
obj = 'Article_obj'+ str(change)


companies = ['Microsoft','Google','Amazon']


def lookup(dic, key, *keys):
     if keys:
         return lookup(dic.get(key, {}), *keys)
     return dic.get(key)


def filebuilder(item):
    filenamelist=[]
    for root,dirs,files in os.walk(directory+item,topdown=False):
        for name in files:
            filenamelist.append(name)
            
    return filenamelist

            
        
def parsing(fileslist,companyname):
    	default = None
	keywords_dict={}
	keywords_count=[]
	global change
	for fileitem in fileslist: 
        	print fileitem
        	dir_company = companyname + '/'
        	raw_objs_string = open(directory+dir_company+fileitem).read()     	#read in raw data
        	raw_objs_string = raw_objs_string.replace('}{', '},{')  		#insert a comma between each object
        	objs_string = '[%s]'%(raw_objs_string)             			#wrap in a list, to make valid json
        	objs = json.loads(objs_string) 

        	for item in objs:
            		for x in item['response']['docs']:
                		obj = articles()
				obj.company = companyname
                		obj.type_of_material = x['type_of_material']
                		obj.news_desk = x['news_desk']
                		obj.lead_paragraph = x['lead_paragraph']
                		if x['abstract'] is None:
                    			obj.abstract = 'Empty'              
                		else:
                    			obj.abstract = x['abstract']
                    		obj._id= x['_id']
                    		obj.snippet = x['snippet']
                    		obj.web_url = x['web_url']
                    		test = x['pub_date'].split('-')
                    		obj.pub_year = test[0]
                    		obj.pub_month = test[1]
				day = test[2]
				obj.pub_day = day[:2]
				obj.source = x['source']
                   		obj.section_name = x['section_name']
                    		obj.subsection_name = x['subsection_name']
            
                    		dict_headline= x.get('headline',default)
				if dict_headline: 
                    			main_part  = lookup(dict_headline,'main')
                    			obj.headline = main_part
				else:
					obj.headline = default
                    		obj.document_type = x['document_type']
                    		obj.word_count = x['word_count']
               
                    		for z in x['keywords']:
        
                 			if z.get('rank',default):
                            			keywords_dict['Rank'] = z['rank']
            
                        		if z.get('is_major',default):
                            			keywords_dict['is_major'] = z['is_major']            
                        		if z.get('name',default):
                            			keywords_dict['Name'] = z['name']
                        		if z.get('value',default):
                            			keywords_dict['Value'] = z['value']

                            		keywords_count.append(keywords_dict)
                            		keywords_dict={}
                    
                    		obj.keywords = keywords_count    
                    		keywords_count = []    
                    
                    		print ' Saving the current object'
                    		print 'Current object ID is',x['_id']
        
                    		obj.save()
        			change = change+1
            			obj = 'Article_obj'+ str(change)
        


def main():
    
    
    for companyname in companies:
        fileslist = filebuilder(companyname)
        print '>>>>>>>>>>>>>>>>>>>>', companyname
	parsing(fileslist,companyname)


if __name__ == '__main__':main()
