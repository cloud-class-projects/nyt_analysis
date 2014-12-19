#!/bin/bash

function usage
{
    echo "Usage: CorrelationPlotter.sh <IP Adress of the server running MongoDB> <Company>"
    echo "Ex: CorrelationPlotter.sh 10.39.1.153 Microsoft"
}

if [ $# -ne 2 ];
then
    usage;
    exit 1
fi

cd /home/ubuntu

echo
echo "Step1: Running CoreNLP Sentiment Analyzer"
echo

hadoop jar jars/sentiment.jar CorpNewsSentimentAnalyzer $1 $2

echo
echo "Step2: Finished generating sentiments for $2. Retrieving sentiment data for $2 from MongoDB."
echo

java -jar jars/sentimentDataRetriever.jar company $2

echo
echo "Step3: Retrieved sentiments for $2. Retrieving stock data for $2 from MongoDB."
echo

java -jar jars/stockDataRetriever.jar $2

echo
echo "Step4: Retrieved stock data for $2. Combining stock and sentiment data for $2."
echo

java -jar jars/stockSentimentCombiner.jar $2

echo
echo "Step5: Running Rscript to plot Stock-Sentiment Correlation."
echo

Rscript rScripts/CorrelationPlotter.r "/home/ubuntu/$2_combined"

echo
echo "Please check the output -> $2_sentiment.pdf"
echo
