package sqlancer;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class SQLConnection implements SQLancerDBConnection {

    private final Connection connection;
    
    private final Connection physicalConnection;

    public SQLConnection(Connection connection) {
        this(connection, null);
    }

    public SQLConnection(Connection connection, Connection physicalConnection) {
        this.connection = connection;
        this.physicalConnection = physicalConnection;
    }

    @Override
    public String getDatabaseVersion() throws SQLException {
        DatabaseMetaData meta = connection.getMetaData();
        return meta.getDatabaseProductVersion();
    }

    @Override
    public void close() throws SQLException {
        connection.close();
        if (physicalConnection != null) {
            physicalConnection.close();
        }
    }

    public Statement prepareStatement(String arg) throws SQLException {
        return connection.prepareStatement(arg);
    }

    public Statement createStatement() throws SQLException {
        return connection.createStatement();
    }

    public Statement createPhysicalStatement() throws SQLException {
        if (physicalConnection != null) {
            return physicalConnection.createStatement();
        }
        return connection.createStatement();
    }

    public Connection getConnection() {
        return connection;
    }

    public Connection getPhysicalConnection() {
        return physicalConnection != null ? physicalConnection : connection;
    }
}
