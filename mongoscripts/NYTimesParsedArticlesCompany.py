import json
from models_companies import articles,parsed_articles # importing the data models to store company information
import os

#Object building  
change= 0
parsed_obj = 'Article_obj'+ str(change)

#companies list
companies = ['Microsoft','Google','Amazon']

# building the parsed_articles collection for comapnies
def parsed_collection(company):
	
	default = None
	global change
	for obj in articles.objects:			#iterating through the article objects
		if obj.company == company:		#Checking to see if the article objects have the company name in question
        		for z in obj.keywords:		#Checking keywords for rank of the article
        			if z.get('Rank',default) == '1' or z.get('Rank',default) == '2':
					# if abstract is not empty and length of the abstract is grater than 45 characters
					if obj.abstract != 'Empty' and len(obj.abstract) > 45:
						# create an object and save the abstract in that object
						parsed_obj = parsed_articles()
						parsed_obj.main_content = obj.abstract 		
						parsed_obj.company = company
						parsed_obj._id =str(obj.id)
						parsed_obj.year = obj.pub_year
						parsed_obj.month = obj.pub_month
						parsed_obj.day = obj.pub_day
						print 'Saving abstract : ID', obj.id
						parsed_obj.save()	 			
                        
					else:	
						# else create an object and save the snippet in that object
                                		parsed_obj = parsed_articles()
						parsed_obj.main_content = obj.snippet
						parsed_obj.company = company
                                        	parsed_obj._id = str(obj.id)
						parsed_obj.year = obj.pub_year
						parsed_obj.month = obj.pub_month
						parsed_obj.day = obj.pub_day
						print 'Saving snippet : ID', obj.id
						parsed_obj.save()
		
			# build a new object and proceed
			change = change + 1
			parsed_obj = 'Article_obj'+ str(change)			
              
                     

def main():


    for companyname in companies:
        parsed_collection(companyname)
        print '>>>>>>>>>>>>>>>>>>>>', companyname
        


if __name__ == '__main__':main()
                       
 
