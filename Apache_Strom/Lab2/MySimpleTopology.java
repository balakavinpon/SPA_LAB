import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class MySimpleTopology {

  //Entry point for the topology
  public static void main(String[] args) throws Exception {

        //Build Topology
	TopologyBuilder builder = new TopologyBuilder();
	builder.setSpout("words", new RandomWordSpout(), 10);
	builder.setBolt("exclaim1", new ExclamationBolt(), 3).shuffleGrouping("words");
	builder.setBolt("exclaim2", new ExclamationBolt(), 2).shuffleGrouping("exclaim1");

	//new configuration
	Config conf = new Config();
	//Set to false to disable debug information when running in production on a cluster
	conf.setDebug(false);
	//Cap the maximum number of executors that can be spawned for a component to 3
	conf.setMaxTaskParallelism(3);

	//LocalCluster is used to run locally
	LocalCluster cluster = new LocalCluster();
	//submit the topology
	cluster.submitTopology("simple-topology", conf, builder.createTopology());

	//sleep
	Thread.sleep(10000);

	//shut down the cluster
	cluster.shutdown();
  }
}