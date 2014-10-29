package second

import backtype.storm.spout.SpoutOutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichSpout
import backtype.storm.tuple.Fields
import backtype.storm.tuple.Values

class XMLReaderSpout extends BaseRichSpout {
    SpoutOutputCollector collector
    String fileWithPathUri

    XMLReaderSpout(String fileWithPathUri) {
        this.fileWithPathUri = fileWithPathUri
    }

    @Override
    void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields('PropertyCode', 'source', 'Stream', 'Qualifier', 'DateTime', 'value'))
    }

    @Override
    void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector
    }

    @Override
    void nextTuple() {
        def Root = new XmlSlurper().parse(fileWithPathUri)
        def allDataPoints = Root.DataPoint
        allDataPoints.each { dataPoint ->
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
    }
}
