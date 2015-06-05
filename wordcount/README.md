# Word Count Sample Application

This is a slightly extended and intentionally complex example of doing a *word count* over complex input data. 

Where complex, in this fabricated case, consists of XML documents packed into single lines in a delimited text file
(for the sake of this example only, not as a best practice).

The intent of this sample application is to show the use of sub-assemblies and a Cascade to coordinate inter-dependent
units of work.

## To Use 

This sample app requires Apache Tez.

To use in Tez local mode:

* make sure Hadoop 2 is in your path,
* Apache Tez is in the CLASSPATH

To use on a cluster:

* make sure Tez is on HDFS within the `apps` folder 
* HADOOP_CONF is set to valid configuration settings

From the downloaded archive execute,

> hadoop jar wordcount.jar data/url+page.200.txt output local

If building from source,

> gradle jar
> hadoop jar ./build/libs/wordcount.jar data/url+page.200.txt output local

Note that if HADOOP_CONF references a cluster, the input files must be copied to HDFS and the 
above 'output' directory will show up in HDFS.

# Helpful Commands

When running in Tez local mode:

```shell
export TEZ_HOME=$BASEDIR/tez-0.6.1
export YARN_HOME=$BASEDIR/hadoop-2.6.0
export HADOOP_HOME=$BASEDIR/hadoop-2.6.0

export YARN_CLASSPATH=$TEZ_HOME/jars/*:$TEZ_HOME/jars/lib/*
export HADOOP_CLASSPATH=$TEZ_HOME/jars/*:$TEZ_HOME/jars/lib/*

export PATH=$YARN_HOME/bin:$PATH
```

With local configuration settings for a remote cluster, add:

```shell
export HADOOP_CONF_DIR=$TEZ_HOME/conf
export YARN_CONF_DIR=$HADOOP_CONF_DIR
```
