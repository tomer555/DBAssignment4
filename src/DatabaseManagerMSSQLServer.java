public class DatabaseManagerMSSQLServer extends DatabaseManager {

    public DatabaseManagerMSSQLServer(String databaseName) {
        super("jdbc:sqlserver://localhost;Instance=SQLEXPRESS;integratedSecurity=true", databaseName, "", "");
    }

    @Override
    public void startConnection() {
        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            super.startConnection();
        } catch (Exception e) {
            System.out.println(String.format("Error starting connection to database '%s'", databaseName));
            System.out.println(e.getMessage());
        }
    }

}
