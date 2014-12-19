#Installation Instructions

*All the installations are to be carried out as* **root** *user*
```
1. Install Hadoop on all the nodes as specified in the Instructions document provided in the class.
Set all the masters and slaves.
**Note: The Java version has to be 8. Otherwise, this project will not run**
2. Increase the JVM heap space to 4GB. In /usr/lib/hadoop/libexec/hadoop-config.sh, 
set JAVA_HEAP_MAX=-Xmx4096m. The 'JAVA_HEAP_MAX' variable has to be updated in 2 places in the file.
This has to be done on all nodes.
3. Install MongoDB on the master node using the commands given below:
   - sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 7F0CEB10
   - echo 'deb http://downloads-distro.mongodb.org/repo/ubuntu-upstart dist 10gen' | sudo tee /etc/apt/sources.list.d/mongodb.list
   - sudo apt-get update
   - sudo apt-get install -y mongodb-org
4. In /etc/mongod.conf, change 'bind_ip = 127.0.0.1' to '#bind_ip = 127.0.0.1'.
   Basically, comment it out so that mongo listens on all IP addresses.
5. Restart MongoDB. -> $ service mongod restart
6. Install mongoengine as show below:
   - apt-get install python-pip
   - pip install mongoengine
```
#Execution Instructions
```
sudo su - root
cd /home/ubuntu
```
```
Copy the following to /home/ubuntu
 - HadoopSentimentAnalyzer
 - MongoSentimentDataRetriever
 - MongoStockDataRetriever
 - StockSentimentCombiner
 - lib
 - CompilerCode
 - rScripts
 - DataLoader.sh
 - NYTimesSentimentAnalysis.sh
 - StockSentimentCorrelation.sh
```
```
- Download StanfordCoreNLP from (link) and place it in lib
- Copy 'mongo-java-driver-2.12.4.jar' and 'mongo-hadoop-core-1.3.0.jar' from '/home/ubuntu/lib'
directory to '/usr/lib/hadoop/lib'. This is to be done on all nodes.
- Restart hadoop services on all nodes.
```
```
cd CompilerCode
chmod 755 compiler.sh
./compiler.sh
cd /home/ubuntu
```
```
chmod 755 DataLoader.sh
chmod 755 NYTimesSentimentAnalysis.sh
chmod 755 StockSentimentCorrelation.sh
```
```
./DataLoader.sh
./NYTimesSentimentAnalysis.sh <IPAdress of the server running MongoDB instance> <Country>
./StockSentimentCorrelation.sh <IPAdress of the server running MongoDB instance> <Company>
```

```
Country and Company names should be specified in CamelCase. (First letter uppercase)
Countries available: China, Japan, Germany.
(Due to space constraints, we have uploaded the data only for these 3 countries so
that it can be used to test the code.)
Companies available: Microsoft, Amazon
```

The output graph is a PDF stored in /home/ubuntu (format: *country/company*_sentiment.pdf)
