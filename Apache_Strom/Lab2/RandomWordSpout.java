import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

//This spout randomly emits words
public class RandomWordSpout extends BaseRichSpout 
{
  //Collector used to emit output
  SpoutOutputCollector _collector;
  //Used to generate a random number
  Random _rand;

  //Open is called when an instance of the class is created
  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) 
  {
	//Set the instance collector to the one passed in
	_collector = collector;
	//For randomness
	_rand = new Random();
  }

  //Emit data to the stream
  @Override
  public void nextTuple() 
  {
    //Sleep for a bit
    Utils.sleep(100);

    //The words that are randomly emitted
    final String[] words = new String[] {"Dubai", "Pilani", "Goa", "Hyderabad"};
    final Random rand = new Random();
    final String word = words[rand.nextInt(words.length)];

    //Emit the sentence
    _collector.emit(new Values(word));
  }

  //Ack is not implemented since this is a basic example
  @Override
  public void ack(Object id) 
  {
  }

  //Fail is not implemented since this is a basic example
  @Override
  public void fail(Object id) 
  {
  }

  //Declare the output fields. In this case, a word
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) 
  {
	declarer.declare(new Fields("word"));
  }
}