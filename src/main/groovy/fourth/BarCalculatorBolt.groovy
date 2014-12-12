package fourth

import backtype.storm.task.OutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.tuple.Fields
import backtype.storm.tuple.Values
import backtype.storm.tuple.Tuple

class BarCalculatorBolt extends BaseRichBolt {
    private OutputCollector collector
    private Double seedRate = 200.99

    @Override
    void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector
    }

    @Override
    void execute(Tuple input) {
        def id = input.getInteger(0)
        def rate = input.getValue(1)
        def transformedRate = bestAvailableRate(rate)
        collector.emit(input, new Values(id, transformedRate))
        collector.ack(input)
    }

    @Override
    void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields('id', 'rate'))
    }

    def bestAvailableRate(rate) {
        if(!rate) seedRate
        else rate
    }
}
