-- SUMMARY --

This is a README file for TermFrequency program.

-- REQUIREMENTS --

HADOOP Environment or Cloudera VM.


-- Running the program --

* Before you run the sample, you must create input and output locations in HDFS. Use the following commands to create the input directory/user/cloudera/TermFrequency/input in HDFS:
$ sudo su hdfs
$ hadoop fs -mkdir /user/cloudera
$ hadoop fs -chown cloudera /user/cloudera
$ exit
$ sudo su cloudera
$ hadoop fs -mkdir /user/cloudera/TermFrequency /user/cloudera/TermFrequency/input 

* Move the text files of the canterbury corpus provided to use as input, and move them to the/user/cloudera/TermFrequency/input directory in HDFS. 

$ hadoop fs -put file* /user/cloudera/TermFrequency/input 

* Compile the TermFrequency class.
To compile in a package installation of CDH:

$ mkdir -p build
$ javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* TermFrequency.java -d build -Xlint 

* Create a JAR file for the TermFrequency application.
$ jar -cvf TermFrequency.jar -C build/ . 

* Run the TermFrequency application from the JAR file, passing the paths to the input and output directories in HDFS.
$ hadoop jar TermFrequency.jar org.myorg.TermFrequency /user/cloudera/TermFrequency/input /user/cloudera/TermFrequency/output

* Output can be seen using the below command:
$ hadoop fs -cat /user/cloudera/TermFrequency/output/*

* If you want to run the sample again, you first need to remove the output directory. Use the following command.
$ hadoop fs -rm -r /user/cloudera/TermFrequency/output 

-- CONTACT --

* Sampath Kumar Gunasekaran (sgunase2@uncc.edu)
