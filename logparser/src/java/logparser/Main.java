/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package logparser;

import java.util.Properties;

import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.regex.RegexParser;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.hadoop.Lfs;
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
    TextLine scheme = new TextLine( new Fields( "offset", "line" ) );

    // create SOURCE tap to read a resource from the local file system, if input is not an URL
    Tap logTap = inputPath.matches( "^[^:]+://.*" ) ? new Hfs( scheme, inputPath ) : new Lfs( scheme, inputPath );

    // create an assembly to parse an Apache log file and store on an HDFS cluster

    // declare the field names we will parse out of the log file
    Fields apacheFields = new Fields( "ip", "time", "method", "event", "status", "size" );

    // define the regular expression to parse the log file with
    String apacheRegex = "^([^ ]*) +[^ ]* +[^ ]* +\\[([^]]*)\\] +\\\"([^ ]*) ([^ ]*) [^ ]*\\\" ([^ ]*) ([^ ]*).*$";

    // declare the groups from the above regex we want to keep. each regex group will be given
    // a field name from 'apacheFields', above, respectively
    int[] allGroups = {1, 2, 3, 4, 5, 6};

    // create the parser
    RegexParser parser = new RegexParser( apacheFields, apacheRegex, allGroups );

    // create the import pipe element, with the name 'import', and with the input argument named "line"
    // replace the incoming tuple with the parser results
    // "line" -> parser -> "ts"
    Pipe importPipe = new Each( "import", new Fields( "line" ), parser, Fields.RESULTS );

    // create a SINK tap to write to the default filesystem
    // by default, TextLine writes all fields out
    Tap remoteLogTap = new Hfs( new TextLine(), outputPath, SinkMode.REPLACE );

    // set the current job jar
    Properties properties = new Properties();
    AppProps.setApplicationJarClass( properties, Main.class );

    // connect the assembly to the SOURCE and SINK taps
    Flow parsedLogFlow = new HadoopFlowConnector( properties ).connect( logTap, remoteLogTap, importPipe );

    // optionally print out the parsedLogFlow to a DOT file for import into a graphics package
    // parsedLogFlow.writeDOT( "logparser.dot" );

    // start execution of the flow (either locally or on the cluster
    parsedLogFlow.start();

    // block until the flow completes
    parsedLogFlow.complete();
    }
  }
