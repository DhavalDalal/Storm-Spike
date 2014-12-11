package fourth

import backtype.storm.task.OutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.tuple.Fields
import backtype.storm.tuple.Tuple
import backtype.storm.tuple.Values
import groovy.json.JsonBuilder


class JsonWriterBolt extends BaseRichBolt {
    private OutputCollector collector
    private JsonBuilder json

    @Override
    void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector
        json = new JsonBuilder()
    }

    @Override
    void execute(Tuple input) {
        def id = input.getString(0)
        def rate = input.getString(1)
        json(id: id, rate: rate)
        collector.emit(input, new Values(json.toString()))
        collector.ack(input)
    }

    @Override
    void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields('json'))
    }
}
