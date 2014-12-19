#!/bin/bash

function usage
{
    echo "Usage: NYTimesSentimentAnalysis.sh <IP Adress of the server running MongoDB> <Country>"
    echo "Ex: NYTimesSentimentAnalysis.sh 10.39.1.153 China"
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

hadoop jar jars/sentiment.jar HadoopSentimentAnalyzer $1 $2

echo
echo "Step2: Finished generating sentiments for $2. Retrieving sentiment data for $2 from MongoDB."
echo

java -jar jars/sentimentDataRetriever.jar country $2

echo
echo "Step3: Running Rscript to plot sentiment data."
echo

Rscript rScripts/CountrySentimentPlotter.R "/home/ubuntu/$2_sentiment"

echo
echo "Please check the output -> $2_sentiment.pdf"
echo
