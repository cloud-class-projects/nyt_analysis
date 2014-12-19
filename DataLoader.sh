cd mongoscripts

echo
echo "Loading countries data into the article collection"
echo
python NYTimesArticlesCountry.py

echo
echo "Loading the countries data in the parsed_article collection"
echo
python NYTimesParsedArticlesCountry.py

echo
echo "Loading the company data into article collection"
echo
python NYTimesArticlesCompany.py

echo
echo "Loading the comapny data into parsed_article collection"
echo
python NYTimesParsedArticlesCompany.py

echo
echo "Loading the companies stock data"
echo
python Stocks.py
