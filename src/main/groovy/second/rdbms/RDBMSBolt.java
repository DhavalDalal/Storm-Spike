package second.rdbms;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;


import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class RDBMSBolt extends BaseRichBolt {
    private OutputCollector collector;

    Map<String, Connection> conMap;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {

        System.out.println("Mysql Bolt = " + input);
        StringBuilder query = new StringBuilder();

        query.append("insert into test.DataPoints(source, stream, qualifier, datetime, value) values (");
        query.append("'").append(input.getStringByField("source")).append("',");
        query.append("'").append(input.getStringByField("Stream")).append("',");
        query.append("'").append(input.getStringByField("Qualifier")).append("',");
        query.append("'").append(input.getStringByField("DateTime")).append("',");
        query.append("'").append(input.getDoubleByField("value")).append("');");

        System.out.println("Mysql Bolt Query = " + query.toString());

        executeUpdate(input.getStringByField("PropertyCode"), query.toString());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    private boolean executeUpdate(String propertyCode , String query){

        Connection con = null;
        Statement stmt = null;
        boolean ok = false;
        try {
            con = getConnction(propertyCode);
            stmt = con.prepareStatement(query);
            ok= stmt.executeUpdate(query) == 0 ? true : false;
            stmt.close();
            con.close();

        } catch( SQLException e){
                e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        finally {
         }
        return ok;
    }
    private Connection getConnction(String propertyCode) throws SQLException, ClassNotFoundException {
        //conMap = new HashMap<String, Connection>();
        Connection con;

        //System.out.println("map = " + conMap.toString());

        //if (null == conMap.get(propertyCode)) {

            Map<String, String> conDetails = lookUpConnectionDetails(propertyCode);

            RDBMSConnector connector = new MysqlRDBMSConnector();


            con = connector.getConnection(
                    conDetails.get("dbServer"),
                    conDetails.get("dbPort"),
                    conDetails.get("dbName"),
                    conDetails.get("dbUser"),
                    conDetails.get("dbPass")
            );

           // conMap.put(propertyCode, con);
        //} else{
          //  con = conMap.get(propertyCode);
        //}

        return con;
    }

    private Map<String, String> lookUpConnectionDetails(String propertyCode) {
        Map<String,String> conDetails = new HashMap<String, String>();

        conDetails.put("dbServer", "localHost");
        conDetails.put("dbPort", "3306");
        conDetails.put("dbName", "test");
        conDetails.put("dbUser", "root");
        conDetails.put("dbPass", "");

        return conDetails;
    }

}
