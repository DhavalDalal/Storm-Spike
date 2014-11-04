package third

import backtype.storm.task.OutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.tuple.Fields
import backtype.storm.tuple.Values
import backtype.storm.tuple.Tuple

class XMLReaderBolt extends BaseRichBolt {
    private OutputCollector collector
    private XmlSlurper xmlSlurper

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector
    }

    @Override
    void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields('PropertyCode', 'source', 'Stream', 'Qualifier', 'DateTime', 'value'))
    }

    @Override
    public void execute(Tuple input) {
        def message = input.getString(0)
        def Root = new XmlSlurper().parseText(message)
        Root.DataPoint.each { dataPoint ->
            Values values = new Values(
                dataPoint.@PropertyCode.text(),
                dataPoint.@source.text(),
                dataPoint.@Stream.text(),
                dataPoint.@Qualifier.text(),
                dataPoint.@DateTime.text(),
                dataPoint.@value.text()
            )
            collector.emit(values)
        }
        collector.ack(input)
    }
}
