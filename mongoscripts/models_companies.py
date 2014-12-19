# This model is same as the country model except change to incorporate the company name

from mongoengine import *
connect('nytimesdb')


class articles(Document):
	
	company = StringField()                   # String field used to store company name
	type_of_material = StringField()
	news_desk = StringField()
	lead_paragraph = StringField()
	abstract = StringField()
	_id = StringField()
	snippet = StringField()
	web_url = StringField()
	pub_year = IntField()
	pub_month = IntField()
	pub_day = IntField()
	source = StringField()
        section_name = StringField()
        subsection_name = StringField()
        headline = StringField()
        document_type = StringField()
        word_count = IntField()	
	keywords = ListField()




class parsed_articles(Document):
	
	_id = StringField()
	company = StringField()			# String fiels used to store the comapny name 
	year = IntField()
	month = IntField()
	day = IntField()
	main_content = StringField()
    
