package second

import backtype.storm.task.OutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.tuple.Tuple
import com.mongodb.BasicDBObjectBuilder
import com.mongodb.DB
import com.mongodb.DBCollection
import com.mongodb.MongoClient
import com.mongodb.ServerAddress
import groovy.json.JsonBuilder
import org.bson.types.BSONTimestamp

class MongoInsertBolt extends BaseRichBolt {
    private OutputCollector collector
    private final String mongoUri
    private final int port
    private final String dbName
    private final String collectionName
    private DBCollection dbCollection

    MongoInsertBolt(String mongoUri, int port, String dbName, String collectionName) {
        this.mongoUri = mongoUri
        this.port = port
        this.dbName = dbName
        this.collectionName = collectionName
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector
        ServerAddress server = new ServerAddress(mongoUri, port)
        MongoClient mongo = new MongoClient(server)
        dbCollection = mongo.getDB(dbName).getCollection(collectionName)
    }

    @Override
    public void execute(Tuple input) {
        def json = new JsonBuilder()
        json PropertyCode: input.getStringByField('PropertyCode'),
                source: input.getStringByField('source'),
                Stream: input.getStringByField('Stream'),
                Qualifier: input.getStringByField('Qualifier'),
                DateTime: input.getStringByField('DateTime'),
                value: input.getDoubleByField('value')

        def record = BasicDBObjectBuilder
                .start()
                .add('ts', new Date())
                .add('PropertyCode', input.getStringByField('PropertyCode'))
                .add('source', input.getStringByField('source'))
                .add('Stream', input.getStringByField('Stream'))
                .add('Qualifier', input.getStringByField('Qualifier'))
                .add('DateTime', input.getStringByField('DateTime'))
                .add('value', input.getDoubleByField('value'))
                .get()
        dbCollection.insert(record)
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
