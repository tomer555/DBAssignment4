import java.sql.*;
public class DatabaseManager {

    protected String connectionString;
    protected String databaseName;
    protected String username;
    protected String password;
    protected Connection conn;


    public DatabaseManager(String connectionString, String databaseName, String username, String password) {
        this.connectionString = connectionString;
        this.databaseName = databaseName;
        this.username = username;
        this.password = password;
    }


    public void startConnection() {
        try {
            conn = DriverManager.getConnection(connectionString, username, password);
            selectDatabase(this.databaseName);
        } catch (Exception e) {
            System.out.println(String.format("Cannot start connection to: %s", connectionString));
            System.out.println(e.getMessage());
        }
    }

    /**
     * Selects database to work on.
     *
     * @return true if the database exists and been selected.
     */
    public void selectDatabase(String databaseName) {
        try {
            Statement stmt = conn.createStatement();
            stmt.execute("use " + databaseName);
            stmt.close();
        } catch (Exception e) {
            System.out.println(String.format("Database '%s' not found at: %s", databaseName, connectionString));
            System.out.println(e.getMessage());
        }
    }

    /**
     * Close the connection
     *
     * @return true if the connection is closed properly
     */
    public void closeConnection() {
        try {
            if (conn != null && !conn.isClosed()) {
                conn.close();
                conn = null;
            }
        } catch (Exception e) {
            System.out.println(String.format("Error closing connection to: %s", connectionString));
            System.out.println(e.getMessage());
        }
    }






}
