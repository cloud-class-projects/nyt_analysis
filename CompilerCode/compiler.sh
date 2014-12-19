mkdir /home/ubuntu/bin
mkdir /home/ubuntu/jars

echo
echo "1. Compiling StockSentimentCombiner"
echo

javac -d /home/ubuntu/bin -cp /home/ubuntu/StockSentimentCombiner/src/StockSentimentCombiner.java

jar -cvfm /home/ubuntu/jars/stockSentimentCombiner.jar StockSentimentCombinerManifest.MF -C /home/ubuntu/bin/ .

rm /home/ubuntu/bin/*



echo
echo "2. Compiling MongoStockDataRetriever"
echo

javac -d /home/ubuntu/bin -cp /home/ubuntu/lib/gson-2.3.1.jar:/home/ubuntu/lib/mongo-java-driver-2.12.4.jar /home/ubuntu/MongoStockDataRetriever/src/MongoStockDataRetriever.java /home/ubuntu/MongoStockDataRetriever/src/StockDataObject.java

jar -cvfm /home/ubuntu/jars/stockDataRetriever.jar StockDataRetrieverManifest.MF -C /home/ubuntu/bin/ .

rm /home/ubuntu/bin/*



echo
echo "3. Compiling MongoSentimentDataRetriever"
echo

javac -d /home/ubuntu/bin -cp /home/ubuntu/lib/gson-2.3.1.jar:/home/ubuntu/lib/mongo-java-driver-2.12.4.jar /home/ubuntu/MongoSentimentDataRetriever/src/CompanyDataObject.java /home/ubuntu/MongoSentimentDataRetriever/src/CountryDataObject.java /home/ubuntu/MongoSentimentDataRetriever/src/MongoSentimentDataRetriever.java

jar -cvfm /home/ubuntu/jars/sentimentDataRetriever.jar SentimentDataRetrieverManifest.MF -C /home/ubuntu/bin/ .

rm /home/ubuntu/bin/*



echo
echo "4. Compiling Hadoop Files"
echo

javac -d /home/ubuntu/bin -cp /home/ubuntu/lib/commons-cli-1.2.jar:/home/ubuntu/lib/ejml-0.23.jar:/home/ubuntu/lib/gson-2.3.1.jar:/home/ubuntu/lib/hadoop-annotations-2.4.1.jar:/home/ubuntu/lib/hadoop-common-2.4.1.jar:/home/ubuntu/lib/hadoop-hdfs-2.4.1.jar:/home/ubuntu/lib/hadoop-mapreduce-client-core-2.4.1.jar:/home/ubuntu/lib/javax.json.jar:/home/ubuntu/lib/joda-time.jar:/home/ubuntu/lib/jollyday.jar:/home/ubuntu/lib/mongo-hadoop-core-1.3.0.jar:/home/ubuntu/lib/mongo-java-driver-2.12.4.jar:/home/ubuntu/lib/stanford-corenlp-3.5.0.jar:/home/ubuntu/lib/stanford-corenlp-3.5.0-models.jar:/home/ubuntu/lib/xom.jar /home/ubuntu/HadoopSentimentAnalyzer/src/CoreNLPSentimentAnalyzer.java /home/ubuntu/HadoopSentimentAnalyzer/src/CorpNewsSentimentAnalyzer.java /home/ubuntu/HadoopSentimentAnalyzer/src/CorpNewsSentimentAnalyzerMapper.java /home/ubuntu/HadoopSentimentAnalyzer/src/CorpNewsSentimentAnalyzerReducer.java /home/ubuntu/HadoopSentimentAnalyzer/src/HadoopSentimentAnalyzer.java /home/ubuntu/HadoopSentimentAnalyzer/src/SentimentAnalyzerMapper.java /home/ubuntu/HadoopSentimentAnalyzer/src/SentimentAnalyzerReducer.java

mkdir /home/ubuntu/temp/
mkdir /home/ubuntu/temp/lib
cp  /home/ubuntu/lib/* /home/ubuntu/temp/lib/
cp /home/ubuntu/bin/* /home/ubuntu/temp/

jar -cvf /home/ubuntu/jars/sentiment.jar -C /home/ubuntu/temp/ .

rm /home/ubuntu/bin/*
rm -r /home/ubuntu/temp/


