/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package wordcount;

import java.util.Properties;

import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.FlowRuntimeProps;
import cascading.flow.tez.Hadoop2TezFlowConnector;
import cascading.operation.Identity;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexFilter;
import cascading.operation.regex.RegexGenerator;
import cascading.operation.regex.RegexReplace;
import cascading.operation.regex.RegexSplitter;
import cascading.operation.xml.TagSoupParser;
import cascading.operation.xml.XPathGenerator;
import cascading.operation.xml.XPathOperation;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.property.AppProps;
import cascading.scheme.hadoop.SequenceFile;
import cascading.scheme.hadoop.TextDelimited;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

/**
 *
 */
public class Main
  {
  private static class ImportCrawlDataAssembly extends SubAssembly
    {
    public ImportCrawlDataAssembly( Pipe previous )
      {
      setPrevious( previous );

      // split the text line into "url" and "raw" with the default delimiter of tab
      RegexSplitter regexSplitter = new RegexSplitter( new Fields( "url", "raw" ) );
      Pipe importPipe = new Each( previous, new Fields( "line" ), regexSplitter );

      // remove all pdf documents from the stream
      importPipe = new Each( importPipe, new Fields( "url" ), new RegexFilter( ".*\\.pdf$", true ) );

      // replace ":nl" with a new line, return the fields "url" and "page" to the stream.
      // discard the other fields in the stream
      RegexReplace regexReplace = new RegexReplace( new Fields( "page" ), ":nl:", "\n" );
      importPipe = new Each( importPipe, new Fields( "raw" ), regexReplace, new Fields( "url", "page" ) );

      setTails( importPipe );
      }
    }

  private static class WordCountSplitAssembly extends SubAssembly
    {
    public WordCountSplitAssembly( Pipe previous, String sinkUrlName, String sinkWordName )
      {
      setPrevious( previous );

      // create a new pipe assembly to create the word count across all the pages, and the word count in a single page

      // convert the html to xhtml using the TagSouParser. return only the fields "url" and "xml", discard the rest
      previous = new Each( previous, new Fields( "page" ), new TagSoupParser( new Fields( "xml" ) ), new Fields( "url", "xml" ) );
      // apply the given XPath expression to the xml in the "xml" field. this expression extracts the 'body' element.
      XPathGenerator bodyExtractor = new XPathGenerator( new Fields( "body" ), XPathOperation.NAMESPACE_XHTML, "//xhtml:body" );
      previous = new Each( previous, new Fields( "xml" ), bodyExtractor, new Fields( "url", "body" ) );
      // apply another XPath expression. this expression removes all elements from the xml, leaving only text nodes.
      // text nodes in a 'script' element are removed.
      String elementXPath = "//text()[ name(parent::node()) != 'script']";
      XPathGenerator elementRemover = new XPathGenerator( new Fields( "words" ), XPathOperation.NAMESPACE_XHTML, elementXPath );
      previous = new Each( previous, new Fields( "body" ), elementRemover, new Fields( "url", "words" ) );
      // apply the regex to break the document into individual words and stuff each word at a new tuple into the current
      // stream with field names "url" and "word"
      RegexGenerator wordGenerator = new RegexGenerator( new Fields( "word" ), "(?<!\\pL)(?=\\pL)[^ ]*(?<=\\pL)(?!\\pL)" );
      previous = new Each( previous, new Fields( "words" ), wordGenerator, new Fields( "url", "word" ) );

      // group on "url"
      Pipe urlCountPipe = new GroupBy( sinkUrlName, previous, new Fields( "url", "word" ) );
      urlCountPipe = new Every( urlCountPipe, new Fields( "url", "word" ), new Count(), new Fields( "url", "word", "count" ) );

      // group on "word"
      Pipe wordCountPipe = new GroupBy( sinkWordName, previous, new Fields( "word" ) );
      wordCountPipe = new Every( wordCountPipe, new Fields( "word" ), new Count(), new Fields( "word", "count" ) );

      setTails( urlCountPipe, wordCountPipe );
      }
    }

  public static void main( String[] args )
    {
    // set the current job jar
    // set the current job jar
    Properties properties = AppProps.appProps()
      .setJarClass( Main.class )
      .buildProperties( );

    properties = FlowRuntimeProps.flowRuntimeProps()
      .setGatherPartitions( 4 ) // level of parallelization during the gather stage
      .buildProperties( properties );

    FlowConnector flowConnector = new Hadoop2TezFlowConnector( properties );

    String inputPath = args[ 0 ];
    String pagesPath = args[ 1 ] + "/pages/";
    String urlsPath = args[ 1 ] + "/urls/";
    String wordsPath = args[ 1 ] + "/words/";
    String localUrlsPath = args[ 2 ] + "/urls/";
    String localWordsPath = args[ 2 ] + "/words/";

    // import a text file with crawled pages from the local filesystem into a Hadoop distributed filesystem
    // the imported file will be a native Hadoop sequence file with the fields "page" and "url"
    // note this examples stores crawl pages as a tabbed file, with the first field being the "url"
    // and the second being the "raw" document that had all new line chars ("\n") converted to the text ":nl:".

    // a predefined pipe assembly that returns fields named "url" and "page"
    Pipe importPipe = new ImportCrawlDataAssembly( new Pipe( "import pipe" ) );

    // create the tap instances
    Tap pagesSource = new Hfs( new TextLine(), inputPath );
    Tap importedPages = new Hfs( new SequenceFile( new Fields( "url", "page" ) ), pagesPath );

    // connect the pipe assembly to the tap instances
    // could optionally use the FlowDef class
    Flow importPagesFlow = flowConnector.connect( "import pages", pagesSource, importedPages, importPipe );

    // a predefined pipe assembly that splits the stream into two named "url pipe" and "word pipe"
    // these pipes could be retrieved via the getTails() method and added to new pipe instances
    SubAssembly wordCountPipe = new WordCountSplitAssembly( new Pipe( "wordcount pipe" ), "url pipe", "word pipe" );

    // create Hadoop sequence files to store the results of the counts
    Tap sinkUrl = new Hfs( new SequenceFile( new Fields( "url", "word", "count" ) ), urlsPath );
    Tap sinkWord = new Hfs( new SequenceFile( new Fields( "word", "count" ) ), wordsPath );

    // with multiple sinks, simpler to use the FlowDef class
    FlowDef countDef = FlowDef.flowDef()
      .setName( "import flow" )
      .addSource( "wordcount pipe", importedPages )
      .addSink( "url pipe", sinkUrl )
      .addSink( "word pipe", sinkWord )
      .addTail( wordCountPipe );

    // wordCountPipe will be recognized as an assembly and handled appropriately
    Flow count = flowConnector.connect( countDef );

    // create an assembly to export the Hadoop sequence file to local text files
    Pipe exportPipe = new Each( "export pipe", new Identity() );

    Tap localSinkUrl = new Hfs( new TextDelimited( Fields.ALL, true, "/t" ), localUrlsPath );
    Tap localSinkWord = new Hfs( new TextDelimited( Fields.ALL, true, "/t" ), localWordsPath );

    // connect up both sinks using the same exportPipe assembly
    Flow exportFromUrl = flowConnector.connect( "export url", sinkUrl, localSinkUrl, exportPipe );
    Flow exportFromWord = flowConnector.connect( "export word", sinkWord, localSinkWord, exportPipe );

    // connect up all the flows, order is not significant
    Cascade cascade = new CascadeConnector().connect( importPagesFlow, count, exportFromUrl, exportFromWord );

    // run the cascade to completion
    cascade.complete();

    System.exit( 0 ); // known issue with Tez 0.5.0 in local mode
    }
  }
