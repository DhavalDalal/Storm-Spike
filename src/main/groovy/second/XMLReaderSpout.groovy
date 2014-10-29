package second

import backtype.storm.spout.SpoutOutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichSpout

class XMLReaderSpout extends BaseRichSpout {
    SpoutOutputCollector collector

    XMLReaderSpout(fileWithPath) {
    }

    @Override
    void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector
    }

    @Override
    void nextTuple() {

    }
}
