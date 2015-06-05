# Log Analysis Sample Application

The intent of this sample application is to extend the `logparser` sample application to perform simple 
analytic functions. More importantly to show how an application can be broken down into multiple logical
**units of work** that can be run together as a larger unit of work via the Cascade class.

Note if you delete the `arrivalrate` directory after running the application once, and re-run the application
with the same directories, the first Flow will be skipped.

## To Use 

This sample app requires Apache Tez.

To use in Tez local mode:

* make sure Hadoop 2 is in your path,
* Apache Tez is in the CLASSPATH

To use on a cluster:

* make sure Tez is on HDFS within the `apps` folder 
* HADOOP_CONF is set to valid configuration settings

From the downloaded archive execute,

> hadoop jar loganalysis.jar data/apache.200.txt output

If building from source,

> gradle jar
> hadoop jar ./build/libs/loganalysis.jar data/apache.200.txt output

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
