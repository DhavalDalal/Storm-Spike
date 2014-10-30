package second.rdbms;


public class MysqlRDBMSConnector extends RDBMSConnector {

    public MysqlRDBMSConnector() {
        super("com.mysql.jdbc.Driver");
    }
}
