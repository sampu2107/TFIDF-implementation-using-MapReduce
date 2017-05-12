-- SUMMARY --

This is a README file for DocWordCount program.

-- REQUIREMENTS --

HADOOP Environment or Cloudera VM.


-- Running the program --

* Before you run the sample, you must create input and output locations in HDFS. Use the following commands to create the input directory/user/cloudera/DocWordCount/input in HDFS:
$ sudo su hdfs
$ hadoop fs -mkdir /user/cloudera
$ hadoop fs -chown cloudera /user/cloudera
$ exit
$ sudo su cloudera
$ hadoop fs -mkdir /user/cloudera/DocWordCount /user/cloudera/DocWordCount/input 

* Move the text files of the canterbury corpus provided to use as input, and move them to the/user/cloudera/DocWordCount/input directory in HDFS. 

$ hadoop fs -put file* /user/cloudera/DocWordCount/input 

* Compile the DocWordCount class.
To compile in a package installation of CDH:

$ mkdir -p build
$ javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* DocWordCount.java -d build -Xlint 

* Create a JAR file for the DocWordCount application.
$ jar -cvf docwordcount.jar -C build/ . 

* Run the DocWordCount application from the JAR file, passing the paths to the input and output directories in HDFS.
$ hadoop jar docwordcount.jar org.myorg.DocWordCount /user/cloudera/DocWordCount/input /user/cloudera/DocWordCount/output

* Output can be seen using the below command:
$ hadoop fs -cat /user/cloudera/DocWordCount/output/*

* If you want to run the sample again, you first need to remove the output directory. Use the following command.
$ hadoop fs -rm -r /user/cloudera/DocWordCount/output 

-- CONTACT --

* Sampath Kumar Gunasekaran (sgunase2@uncc.edu)
