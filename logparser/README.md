# Log Parser Sample Application

The intent of this sample application is to show a very simple example of reading a consistently formatted data file
and parsing it into a clean and canonical form. A very typical starting point for many ETl and log processing 
applications.

The use of regular expressions to parse the log entries are for convenience of the example, and not a feature of the 
sample and not a requirement of Cascading. Sometimes it may make sense to write custom functions for performance 
reasons, see the user guide for references.

## To Use 

This sample app requires Apache Tez.

To use in Tez local mode:

* make sure Hadoop 2 is in your path,
* Apache Tez is in the CLASSPATH

To use on a cluster:

* make sure Tez is on HDFS within the `apps` folder 
* HADOOP_CONF is set to valid configuration settings

From the downloaded archive execute,

> hadoop jar logparser.jar data/apache.200.txt output

If building from source,

> gradle jar
> hadoop jar ./build/libs/logparser.jar data/apache.200.txt output

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
