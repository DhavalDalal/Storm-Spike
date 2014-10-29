package first;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class InMemoryWordSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private List<String> words = Arrays.asList("storm", "spike", "for", "playing", "around");
    private Random random = new Random();


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        Utils.sleep(100);
        int idx = random.nextInt(words.size());
        collector.emit(new Values(words.get(idx)));
    }
}
