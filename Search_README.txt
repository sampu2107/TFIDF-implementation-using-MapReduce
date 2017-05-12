-- SUMMARY --

This is a README file for Search program.

-- REQUIREMENTS --

HADOOP Environment or Cloudera VM.


-- Running the program --

* Before you run the sample, you must create input and output locations in HDFS. Output files of the TFIDF batch job would be the input for the search job.
$ sudo su hdfs
$ hadoop fs -mkdir /user/cloudera
$ hadoop fs -chown cloudera /user/cloudera
$ exit
$ sudo su cloudera

* Compile the Search class.
To compile in a package installation of CDH:

$ mkdir -p build
$ javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* Search.java -d build -Xlint 

* Create a JAR file for the Search application.
$ jar -cvf Search.jar -C build/ . 

* Run the Search application from the JAR file, by passing the paths to the output path of TFIDF as the input for Search and enter the query to be searched for in the document. 
$ hadoop jar Search.jar org.myorg.Search /user/cloudera/TFIDF/output /user/cloudera/Search/output

* Output can be seen using the below command:
$ hadoop fs -cat /user/cloudera/Search/output/*

* If you want to run the sample again, you first need to remove the output directory. Use the following command.
$ hadoop fs -rm -r /user/cloudera/Search/output 

-- CONTACT --

* Sampath Kumar Gunasekaran (sgunase2@uncc.edu)
