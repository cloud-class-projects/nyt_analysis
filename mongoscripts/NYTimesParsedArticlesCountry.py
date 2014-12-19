import json
from models import articles,parsed_articles # importing of the countries data model
import os

# counter for object building
change= 0
parsed_obj = 'Article_obj'+ str(change)

#countries list
countries = ['India','China','Japan','North Korea','South Korea','Afghanistan','Taiwan','Venezuela','Canada','Australia','Nigeria','Iran','Iraq','Russia','Brazil','Thailand','Cuba','France','Germany','Italy','Israel']

#method for building the parsed_articles collection by utilizing the articles collection
def parsed_collection(country):
	
	default = None
	global change
	for obj in articles.objects:		#iterating through all article objects
        
		for z in obj.keywords:		#iterating through the keywords in those particular objects
        		
			'''Check to see if the name is glocations, value is the country name and ranking of the article 
			with respect to that particular country'''

			if z.get('Name',default) == 'glocations' and  (z.get('Rank',default) == '1' or 
			z.get('Rank',default) == '2'):
                        	if z.get('Value',default) == country:
					# Check to see if abstract is empty and the length of the abstract is greater than 45
					if obj.abstract != 'Empty' and len(obj.abstract) > 45:
						# if true create an object save the abstract in that object
						parsed_obj = parsed_articles()
						parsed_obj.main_content = obj.abstract 		
						parsed_obj.country = country
						parsed_obj._id =str(obj.id)
						parsed_obj.year = obj.pub_year
						parsed_obj.month = obj.pub_month
						parsed_obj.day = obj.pub_day
						print 'Saving abstract : ID', obj.id
						parsed_obj.save()
						print 'Saving>>>>>>'	 			
                        
					else:	
						# else create a object and save the snippet in that object
                                		parsed_obj = parsed_articles()
						parsed_obj.main_content = obj.snippet
						parsed_obj.country = country
                                                parsed_obj._id = str(obj.id)
						parsed_obj.year = obj.pub_year
						parsed_obj.month = obj.pub_month
						parsed_obj.day = obj.pub_day
						print 'Saving snippet : ID', obj.id
						parsed_obj.save()
						print 'Saving>>>>>>'
		
		# building a new object
		change = change + 1
		parsed_obj = 'Article_obj'+ str(change)			
		            
                     

def main():


    for countryname in countries:
        parsed_collection(countryname)
        print '>>>>>>>>>>>>>>>>>>>>', countryname
        


if __name__ == '__main__':main()
                       
 
