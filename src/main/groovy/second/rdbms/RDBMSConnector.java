package second.rdbms;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Created by pgirga on 10/30/2014.
 */
public class RDBMSConnector {
    private Connection con = null;
    private StringBuilder dbUri = null;

    public RDBMSConnector(String dbClass) {
        this.dbClass = dbClass;
    }

    private String dbClass = "";

    public Connection getConnection(final String dbServer, final String dbPort, final String dbName , final String dbUser, final String dbPass) throws SQLException, ClassNotFoundException {

        dbUri = new StringBuilder();

        //"jdbc:mysql://localhost:3306/testDB"
        dbUri.append("jdbc:mysql://").append(dbServer).append(":").append(dbPort).append("/").append(dbName);
        dbUri.append("?user=").append(dbUser).append("&password=").append(dbPass);
        Class.forName(dbClass);
        con = DriverManager.getConnection(dbUri.toString());
        return con;
    }
}
