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
    private Map<Integer, Double> rates
    private Integer homeHotelId
    private WebBarRateGenerator rateGenerator

    BarCalculatorBolt(homeHotelId) {
        this.homeHotelId = homeHotelId
    }

    @Override
    void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector
        this.rateGenerator = new WebBarRateGenerator()
        rates = [:].withDefault { seedRate }
    }

    @Override
    void execute(Tuple input) {
        def id = input.getInteger(0)
        def rate = input.getValue(1)
        rates[id] = rate
        if(id != homeHotelId)  {
            def newHomeHotelRate = bestAvailableRate(id, rate)
            collector.emit(input, new Values(homeHotelId, newHomeHotelRate))
            collector.emit(input, new Values(id, rate))
        }   else {
            collector.emit(input, new Values(id, rate))
        }
        collector.ack(input)
    }

    @Override
    void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields('id', 'rate'))
    }

    def bestAvailableRate(id, rate) {
        if(!rate) rates[id]
        else rateGenerator.generateWebBAR(rates.values() as List)
    }
}
