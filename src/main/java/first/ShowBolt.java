package first;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.apache.log4j.Logger;

import java.util.Map;

public class ShowBolt extends BaseRichBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String word = input.getString(0);
        String upperCasedWord = input.getString(1);
        String message = String.format("Word: %s, Capitalized: %s", word, upperCasedWord);
        System.out.println(message);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
