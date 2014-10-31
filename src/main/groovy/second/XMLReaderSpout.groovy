package second

import backtype.storm.spout.SpoutOutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichSpout
import backtype.storm.tuple.Fields
import backtype.storm.tuple.Values

class XMLReaderSpout extends BaseRichSpout {
    private SpoutOutputCollector collector
    private final String fileWithPathUri
    private def Root
    boolean itsOver = false;

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
        Root = new XmlSlurper().parse(fileWithPathUri)
    }

    @Override
    void nextTuple() {
        if  (!itsOver && Root != null) {
            def allDataPoints = Root.DataPoint

            println "The XML size = ${allDataPoints.size()}"
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
                itsOver = true
                Root = null
            }
        } else if  (itsOver && Root == null) {
                sleep(5000)
                Values values = new Values("Roger! It is Over", "a", "b", "c", "d", "100")
                collector.emit(values)
        }

    }
}
