
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * This topology reads a file and counts the words in that file, then finds the top N words.
 */
public class TopWordFinderTopology {

  private static final int N = 10;

  public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      throw new IllegalArgumentException("the number of arguments is not correct");
    }

    TopologyBuilder builder = new TopologyBuilder();

    Config config = new Config();
    config.setDebug(true);


    /*
    ----------------------TODO-----------------------
    Task: wire up the topology


    ------------------------------------------------- */



    config.setMaxTaskParallelism(3);

    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("word-count", config, builder.createTopology());

    //wait till the file is read completely
    Thread.sleep(10 * 60 * 1000);

    cluster.shutdown();
  }
}