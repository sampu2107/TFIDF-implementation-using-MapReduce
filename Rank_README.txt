-- SUMMARY --

This is a README file for Rank program.

-- REQUIREMENTS --

HADOOP Environment or Cloudera VM.


-- Running the program --

* Before you run the sample, you must create input and output locations in HDFS. Output files of the Search batch job would be the input for the Rank job.
$ sudo su hdfs
$ hadoop fs -mkdir /user/cloudera
$ hadoop fs -chown cloudera /user/cloudera
$ exit
$ sudo su cloudera

* Compile the Rank class.
To compile in a package installation of CDH:

$ mkdir -p build
$ javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* Rank.java -d build -Xlint 

* Create a JAR file for the Rank application.
$ jar -cvf Rank.jar -C build/ . 

* Run the Rank application from the JAR file, by passing the paths to the output path of TFIDF as the input for Rank and enter the query to be Ranked for in the document. 
$ hadoop jar Rank.jar org.myorg.Rank /user/cloudera/Search/output /user/cloudera/Rank/output

* Output can be seen using the below command:
$ hadoop fs -cat /user/cloudera/Rank/output/*

* If you want to run the sample again, you first need to remove the output directory. Use the following command.
$ hadoop fs -rm -r /user/cloudera/Rank/output 

-- CONTACT --

* Sampath Kumar Gunasekaran (sgunase2@uncc.edu)
