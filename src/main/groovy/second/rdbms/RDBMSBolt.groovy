package second.rdbms;


import backtype.storm.task.OutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.tuple.Tuple


import java.sql.Connection
import java.sql.SQLException
import java.sql.Statement

public class RDBMSBolt extends BaseRichBolt {
    private Connections = [:]
    private OutputCollector collector


    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector
    }

    @Override
    public void execute(Tuple input) {
        def query =
                """
                 |insert into test.DataPoints(source, stream, qualifier, datetime, value)
                 |values('${input.getStringByField('source')}',
                 |       '${input.getStringByField('Stream')}',
                 |       '${input.getStringByField('Qualifier')}',
                 |       '${input.getStringByField('DateTime')}',
                 |       '${input.getDoubleByField('value')}'
                 |);
                """.stripMargin()
        executeUpdate(input.getStringByField("PropertyCode"), query.toString())
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    private void executeUpdate(String propertyCode , String query){
        try {
            Connection con = getConnection(propertyCode)
            Statement stmt = con.prepareStatement(query)
            stmt.executeUpdate(query) == 0 ? true : false;
            stmt.close();
            //con.close();
        } catch( SQLException e){
            e.printStackTrace()
        } catch (ClassNotFoundException e) {
            e.printStackTrace()
        }
    }

    private Connection getConnection(String propertyCode){
        if (null == Connections[propertyCode]){
            println 'Not Found in the Map'
            Connections[propertyCode] = getNewConnection(propertyCode)
        }

        Connections[propertyCode]
    }

    private Connection getNewConnection(String propertyCode) throws SQLException, ClassNotFoundException {
        Connection con
        Map<String, String> conDetails = lookUpConnectionDetails(propertyCode)
        RDBMSConnector connector = new MysqlRDBMSConnector()
        con = connector.getConnection(
                conDetails['dbServer'],
                conDetails['dbPort'],
                conDetails['dbName'],
                conDetails['dbUser'],
                conDetails['dbPass']
        )
        return con
    }

    private Map<String, String> lookUpConnectionDetails(String propertyCode) {
        ['dbServer': 'localHost',
         'dbPort'  : '3306',
         'dbName'  : 'test',
         'dbUser'  : 'root',
         'dbPass'  : ''
        ]
    }
}
