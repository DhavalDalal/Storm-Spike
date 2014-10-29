package second

import backtype.storm.task.OutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.tuple.Fields
import backtype.storm.tuple.Tuple
import backtype.storm.tuple.Values

class TaxBolt extends BaseRichBolt {
    private Map<String, Double> taxRates = [:]
    private OutputCollector collector

    TaxBolt(taxRates = [:]) {
        this.taxRates = taxRates
    }

    @Override
    void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector
    }

    @Override
    void execute(Tuple input) {
        def propertyCode = input.getString(0)
        def source = input.getString(1)
        def stream = input.getString(2)
        def qualifier = input.getString(3)
        def dateTime = input.getString(4)
        def value = Double.parseDouble(input.getString(5))
        def valueAfterTaxDeduction = value / ((100 + taxRates[propertyCode]) / 100)
        collector.emit(input, new Values(propertyCode,
                                        source,
                                        stream,
                                        qualifier,
                                        dateTime,
                                        valueAfterTaxDeduction))
        collector.ack(input)
    }

    @Override
    void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields('PropertyCode', 'source', 'Stream', 'Qualifier', 'DateTime', 'value'))
    }
}
