import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class WordCountTopology {

  //Entry point for the topology
  public static void main(String[] args) throws Exception
  {
    //Used to build the topology
    TopologyBuilder builder = new TopologyBuilder();

    //Add the spout, with a name of 'spout'and parallelism hint of 5 executors
    builder.setSpout("spout", new RandomSentenceSpout(), 5);

    //Add the SplitSentence bolt, with a name of 'split' and parallelism hint of 8 executors
    //shufflegrouping subscribes to the spout, and equally distributes
    //tuples (sentences) across instances of the SplitSentence bolt
    builder.setBolt("split", new SplitSentence(), 8).shuffleGrouping("spout");

    //Add the counter, with a name of 'count' and parallelism hint of 12 executors
    //fieldsgrouping subscribes to the split bolt, and ensures that the same word is sent to the same instance (group by field 'word')
    builder.setBolt("count", new WordCount(), 12).fieldsGrouping("split", new Fields("word"));

    //new configuration
    Config conf = new Config();
    //Set to false to disable debug information when running in production on a cluster
    conf.setDebug(false);
    //Cap the maximum number of executors that can be spawned for a component to 3
    conf.setMaxTaskParallelism(3);

    //LocalCluster is used to run locally
    LocalCluster cluster = new LocalCluster();
    //submit the topology
    cluster.submitTopology("word-count", conf, builder.createTopology());
    //sleep
    Thread.sleep(10000);
    //shut down the cluster
    cluster.shutdown();
  }
}