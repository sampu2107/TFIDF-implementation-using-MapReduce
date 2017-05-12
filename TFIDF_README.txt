-- SUMMARY --

This is a README file for TFIDF program.

-- REQUIREMENTS --

HADOOP Environment or Cloudera VM.


-- Running the program --

* Before you run the sample, you must create input and output locations in HDFS. Use the following commands to create the input directory/user/cloudera/TFIDF/input in HDFS:
$ sudo su hdfs
$ hadoop fs -mkdir /user/cloudera
$ hadoop fs -chown cloudera /user/cloudera
$ exit
$ sudo su cloudera
$ hadoop fs -mkdir /user/cloudera/TFIDF /user/cloudera/TFIDF/input 

* Move the text files of the canterbury corpus provided to use as input, and move them to the/user/cloudera/TermFrequency/input directory in HDFS. 

$ hadoop fs -put file* /user/cloudera/TermFrequency/input 

* Compile the TFIDF class.
To compile in a package installation of CDH:

$ mkdir -p build
$ javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/*:. TFIDF.java -d build -Xlint 

* Create a JAR file for the TFIDF application.
$ jar -cvf TFIDF.jar -C build/ . 

* Run the TFIDF application from the JAR file, by passing the paths to the input for TermFrequency and the output path of TermFrequency as input to the TFIDF job.
$ hadoop jar TFIDF.jar org.myorg.TFIDF /user/cloudera/TermFrequency/input /user/cloudera/TermFrequency/output /user/cloudera/TFIDF/output

* Output can be seen using the below command:
$ hadoop fs -cat /user/cloudera/TFIDF/output/*

* If you want to run the sample again, you first need to remove the output directory. Use the following command.
$ hadoop fs -rm -r /user/cloudera/TFIDF/output 

-- CONTACT --

* Sampath Kumar Gunasekaran (sgunase2@uncc.edu)
