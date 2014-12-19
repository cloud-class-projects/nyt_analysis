#importing mongoengine features
from mongoengine import *

#Connects to mongodb database if it exista else creates one
connect('nytimesdb')

#Data model for articles, articles will the collection name in mongodb
class articles(Document):
	
	country = StringField()			#String field to store country name
	type_of_material = StringField()	#String field to store type of material
	news_desk = StringField()		#String field to store news desk Example:International or local	
	lead_paragraph = StringField()		#String field to store lead paragraph of the articles
	abstract = StringField()		#String field to store the abstract of the articles
	_id = StringField()			#String field to store Article ID
	snippet = StringField()			#String field to store article snippet
	web_url = StringField()			#String field to store Web URL of the published article
	pub_year = IntField()			#Integer field to store publication year of the articled
	pub_month = IntField()			#Integer field to store the article publication month
	pub_day = IntField()			#Integer field to store the article publication day
	source = StringField()			#String field to store the article source Example: Other news agencies
        section_name = StringField()		#String field to store section name
        subsection_name = StringField()		#String field to store sub-section name
        headline = StringField()		#String field to store the headline
        document_type = StringField()		#String field to store the document type like article,blog,reviews
        word_count = IntField()			#Integer field to to store the word count in the articles
	keywords = ListField()			#Keywords is a list field having a list of keywords



#Data model for parsed articles, parsed_articles will be the collection name in mongodb
class parsed_articles(Document):
	
	_id = StringField()			#String field for article ID
	country = StringField()			#String field to store country name
	year = IntField()			#String field to the publication year 
	month = IntField()			#publication month
	day = IntField()			#publication day
	main_content = StringField()	#String field that stores the snippet or abstract based on the size of the article 
    
