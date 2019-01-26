package kafka.atomix

import joptsimple.OptionParser
import kafka.utils.{CommandLineUtils, Exit, Logging}
import org.apache.kafka.common.utils.{Time, Utils}

import scala.collection.JavaConverters._
import scala.collection.Seq

object ClusterCommand extends Logging {
  def main(args: Array[String]): Unit = {
    val opts = new ClusterCommandOptions( args )
    if ( args.length == 0 ) {
      CommandLineUtils.printUsageAndDie( opts.parser, "Manage content of cluster state" )
    }
    val actions = Seq( opts.describeOpt ).count( opts.options.has )
    if ( actions != 1 ) {
      CommandLineUtils.printUsageAndDie(
        opts.parser, "Command must include exactly one action: --describe"
      )
    }
    opts.checkArgs()

    val client = new AtomixClient(
      Time.SYSTEM, opts.options.valueOf( opts.atomixConfigOpt ), 60000, 60000, admin = true
    )
    var exitCode = 0
    try {
      if ( opts.options.has( opts.describeOpt ) ) {
        describeKey( client, opts )
      }
    }
    catch {
      case e: Throwable =>
        println( "Error while executing command: " + e.getMessage )
        error( Utils.stackTrace( e ) )
        exitCode = 1
    }
    finally {
      client.close()
      Exit.exit( exitCode )
    }
  }

  def describeKey(client: AtomixClient, opts: ClusterCommandOptions): Unit = {
    for ( key <- opts.options.valuesOf( opts.keysOpt ).asScala ) {
      val includeChildren = opts.options.has( opts.includeChildrenOpt )
      client.clusterState.entrySet().asScala.filter(
        e => e.getKey == key || ( includeChildren && e.getKey.startsWith( key + "/" ) )
      ).toSeq.sortBy(f => f.getKey).foreach(
        e => println( e.getValue.version() + " " + e.getKey + " " + e.getValue.value() )
      )
    }
  }

  class ClusterCommandOptions(args: Array[String]) {
    val parser = new OptionParser(false)

    val atomixConfigOpt = parser.accepts( "atomix", "Absolute path to Atomix configuration file")
      .withRequiredArg.describedAs( "file" ).ofType( classOf[String] )
    val describeOpt = parser.accepts( "describe", "Show content of given keys" )
    val keysOpt = parser.accepts( "keys", "Specify keys of interest (comma separated)" )
      .withRequiredArg.withValuesSeparatedBy( "," ).describedAs( "key" ).ofType( classOf[String] )
    val includeChildrenOpt = parser.accepts( "include-children", "Include children of given key in the output" )

    val options = parser.parse( args : _* )

    def checkArgs() {
      CommandLineUtils.checkRequiredArgs( parser, options, atomixConfigOpt )
      if ( options.has( describeOpt ) ) {
        CommandLineUtils.checkRequiredArgs( parser, options, keysOpt )
      }
    }
  }
}
