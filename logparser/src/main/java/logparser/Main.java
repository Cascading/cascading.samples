/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package logparser;

import java.util.Properties;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.FlowRuntimeProps;
import cascading.flow.tez.Hadoop2TezFlowConnector;
import cascading.operation.regex.RegexParser;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

/**
 *
 */
public class Main
  {
  public static void main( String[] args )
    {
    String inputPath = args[ 0 ];
    String outputPath = args[ 1 ];

    // define what the input file looks like, "offset" is bytes from beginning
    TextLine rawLogScheme = new TextLine( new Fields( "offset", "line" ) );

    // create SOURCE tap to read a resource from the current file system or HTTP URL
    Tap logTap = new Hfs( rawLogScheme, inputPath );

    // create an assembly to parse an Apache log file and store on an HDFS cluster

    // declare the field names we will parse out of the log file
    Fields apacheFields = new Fields( "ip", "time", "method", "event", "status", "size" );

    // define the regular expression to parse the log file with
    String apacheRegex = "^([^ ]*) +[^ ]* +[^ ]* +\\[([^]]*)\\] +\\\"([^ ]*) ([^ ]*) [^ ]*\\\" ([^ ]*) ([^ ]*).*$";

    // create the parser
    RegexParser parser = new RegexParser( apacheFields, apacheRegex );

    // create the import pipe element, with the name 'import', and with the input argument named "line"
    // replace the incoming tuple with the parser results
    // "line" -> parser -> "ts"
    Pipe importPipe = new Each( "import", new Fields( "line" ), parser, Fields.RESULTS );

    // define the output, include a header with field names dynamically, TAB delimited
    TextDelimited parsedLogScheme = new TextDelimited( Fields.ALL, true, "\t" );

    // create a SINK tap to write to the default filesystem
    Tap parsedLogTap = new Hfs( parsedLogScheme, outputPath, SinkMode.REPLACE );

    Properties properties = AppProps.appProps()
      .setJarClass( Main.class ) // set the current job jar
      .buildProperties();

    properties = FlowRuntimeProps.flowRuntimeProps()
      .setGatherPartitions( 4 ) // level of parallelization during the gather stage
      .buildProperties( properties );

    // define the assembly
    FlowDef flowDef = FlowDef.flowDef()
      .addSource( "import", logTap )
      .addSink( "import", parsedLogTap )
      .addTail( importPipe );

    // connect the assembly to the SOURCE and SINK taps
    Flow parsedLogFlow = new Hadoop2TezFlowConnector( properties ).connect( flowDef );

    // optionally print out the parsedLogFlow to a DOT file for import into a graphics package
    // parsedLogFlow.writeDOT( "logparser.dot" );

    // start execution of the flow (either locally or on the cluster)
    parsedLogFlow.start();

    // block until the flow completes
    parsedLogFlow.complete();

    System.exit( 0 ); // known issue with Tez 0.5.0 in local mode
    }
  }
