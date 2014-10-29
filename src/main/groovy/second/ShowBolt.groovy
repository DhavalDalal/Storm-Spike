package second

import backtype.storm.task.OutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.tuple.Tuple

public class ShowBolt extends BaseRichBolt {
    private OutputCollector collector

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector
    }

    @Override
    public void execute(Tuple input) {
        System.out.println(input)
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
