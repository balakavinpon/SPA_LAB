import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import static org.apache.storm.kafka.spout.FirstPollOffsetStrategy.EARLIEST;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import java.util.Properties;

import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import org.apache.storm.kafka.spout.*;
import org.apache.storm.kafka.bolt.*;

public class MyKafkaTopology {

  public static void main(String[] args) throws Exception {

	TopologyBuilder builder = new TopologyBuilder();

	builder.setSpout("kafka_spout", new KafkaSpout<>(
		KafkaSpoutConfig.builder("127.0.0.1:9092", "t1").setProp(ConsumerConfig.GROUP_ID_CONFIG, "kafkaSpoutTestGroup").build()), 1);

	builder.setBolt("bolt", new MyKafkaBolt()).shuffleGrouping("kafka_spout");

        /* The output field of the spout ("lambda") is provided as the boltMessageField
          so that this gets written out as the message in the kafka topic.
          The tuples have no key field, so the messages are written to Kafka without a key.*/
        final KafkaBolt<String, String> kafka_bolt = new KafkaBolt<String, String>()
            .withProducerProperties(newProps("127.0.0.1:9092", "t2"))
            .withTopicSelector(new DefaultTopicSelector("t2"))
            .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper<>("key", "enriched_word"));

        builder.setBolt("forwardToKafka", kafka_bolt, 1).shuffleGrouping("bolt");


        Config conf = new Config();
	conf.setDebug(false);
        conf.setMaxTaskParallelism(3);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("kafka-topology", conf, builder.createTopology());
        Thread.sleep(10000);
        //cluster.shutdown();
  }

    /**
     * Create the Storm config.
     * @return the Storm config for the topology that publishes random UUIDs to Kafka using a Kafka bolt.
     */
    private static Properties newProps(final String brokerUrl, final String topicName) {
        return new Properties() {
            {
                put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
                put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
                put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
                put(ProducerConfig.CLIENT_ID_CONFIG, topicName);
            }
        };
    }

}