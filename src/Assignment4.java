import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;

import javafx.util.Pair;


import java.util.ArrayList;



public class Assignment4 {
    private DatabaseManager manager;
    private Assignment4() {
        //Establish connection to DB2019_Ass2
        this.manager=new DatabaseManagerMSSQLServer("DB2019_Ass2");
        manager.startConnection();



    }

   public static void executeFunc(Assignment4 ass, String[] args) {
        String funcName = args[0];
        switch (funcName) {
            case "loadNeighborhoodsFromCsv":
                ass.loadNeighborhoodsFromCsv(args[1]);
                break;
            case "dropDB":
                ass.dropDB();
                break;
            case "initDB":
                ass.initDB(args[1]);
                break;
            case "updateEmployeeSalaries":
                ass.updateEmployeeSalaries(Double.parseDouble(args[1]));
                break;
            case "getEmployeeTotalSalary":
                System.out.println(ass.getEmployeeTotalSalary());
                break;
            case "updateAllProjectsBudget":
                ass.updateAllProjectsBudget(Double.parseDouble(args[1]));
                break;
            case "getTotalProjectBudget":
                System.out.println(ass.getTotalProjectBudget());
                break;
            case "calculateIncomeFromParking":
                System.out.println(ass.calculateIncomeFromParking(Integer.parseInt(args[1])));
                break;
            case "getMostProfitableParkingAreas":
                System.out.println(ass.getMostProfitableParkingAreas());
                break;
            case "getNumberOfParkingByArea":
                System.out.println(ass.getNumberOfParkingByArea());
                break;
            case "getNumberOfDistinctCarsByArea":
                System.out.println(ass.getNumberOfDistinctCarsByArea());
                break;
            case "AddEmployee":
                SimpleDateFormat format = new SimpleDateFormat("dd-MM-yyyy");
                ass.AddEmployee(Integer.parseInt(args[1]), args[2], args[3], java.sql.Date.valueOf(args[4]), args[5], Integer.parseInt(args[6]), Integer.parseInt(args[7]), args[8]);
                break;
            default:
                break;
        }
    }



    public static void main(String[] args) {
        Assignment4 ass = new Assignment4();
        System.out.println(ass.calculateIncomeFromParking(2018));
        /*
        File file = new File(".");
        String csvFile = args[0];
        String line = "";
        String cvsSplitBy = ",";
        Assignment4 ass = new Assignment4();
        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {

            while ((line = br.readLine()) != null) {

                // use comma as separator
                String[] row = line.split(cvsSplitBy);
                executeFunc(ass, row);

            }

        } catch (IOException e) {
            e.printStackTrace();

        }
        */

    }

    public DatabaseManager getManager(){
        return manager;
    }

    //Revise
    private void loadNeighborhoodsFromCsv(String csvPath) {
        String line;
        String cvsSplitBy = ",";
        String insertQuary="INSERT INTO Neighborhood " + "VALUES (?,?)";
        try (BufferedReader br = new BufferedReader(new FileReader(csvPath))) {
            PreparedStatement statement=manager.conn.prepareStatement(insertQuary);
            while ((line = br.readLine()) != null) {
                // use comma as separator
                String[] row = line.split(cvsSplitBy);
                //insert row into Neighborhood
                statement.setInt(1,Integer.parseInt(row[0]));
                statement.setString(2,row[1]);
                statement.execute();
            }
        } catch (IOException | SQLException e) {
            e.printStackTrace();
        }
    }
    //Revise
    private void updateEmployeeSalaries(double percentage) {
        String query="UPDATE ConstructionEmployeeOverFifty SET SalaryPerDay=SalaryPerDay*?";
        applyPercentage(query,percentage);
    }

    //Revise
    public void updateAllProjectsBudget(double percentage) {
        String query="UPDATE Project SET Budget=Budget*?";
        applyPercentage(query,percentage);

    }
    //Revise
    private void applyPercentage(String query, double percentage){
        PreparedStatement statement;
        try {
            statement = manager.conn.prepareStatement(query);
            statement.setDouble(1,1+(percentage/100));
            statement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    //Revise
    public double getEmployeeTotalSalary() {
        String query="SELECT SUM(SalaryPerDay) as TotalSalary FROM ConstructorEmployee";
        PreparedStatement statement;
        double output=-1;
        try {
            statement = manager.conn.prepareStatement(query);
            ResultSet result = statement.executeQuery();
            if (result.next()) {
                String value = result.getString(1) == null ? "NULL" : result.getString(1);
                output=Double.parseDouble(value);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return output*30;
    }

    //Revise
    private int getTotalProjectBudget() {
        String query="SELECT SUM(Budget) FROM Project";
        PreparedStatement statement;
        int output=-1;
        try {
            statement = manager.conn.prepareStatement(query);
            ResultSet result = statement.executeQuery();
            if (result.next()) {
                String sum = result.getString(1) == null ? "NULL" : result.getString(1);
                output=Integer.parseInt(sum);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return output;
    }
    //Revise
    private void dropDB() {
        String drop = "DROP DATABASE DB2019_Ass2";
        try {
            Statement statement = manager.conn.createStatement();
            statement.executeUpdate(drop);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    //Revise
    private void initDB(String csvPath) {
        ScriptRunner runner = new ScriptRunner(manager.conn, true, true);
        try {
            runner.runScript(new BufferedReader(new FileReader(csvPath)));
        } catch (IOException | SQLException e) {
            e.printStackTrace();
        }
    }
    private int calculateIncomeFromParking(int year) {
        String query="SELECT SUM(Cost) FROM CarParking WHERE StartTime>=? AND EndTime<=?";
        PreparedStatement statement;
        int output=-1;
        try {
            statement = manager.conn.prepareStatement(query);
            statement.setString(1,year+"-01-01");
            statement.setString(2,year+"-12-31");
            ResultSet result = statement.executeQuery();
            if (result.next()) {
                String sum = result.getString(1) == null ? "NULL" : result.getString(1);
                output=Integer.parseInt(sum);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return output;
    }
    //Revise
    private ArrayList<Pair<Integer, Integer>> getMostProfitableParkingAreas() {
        String query="SELECT TOP(5) AID, SUM(Cost) as Earnings FROM ParkingArea as pa JOIN  CarParking as cp\n" +
                "ON pa.AID=cp.ParkingAreaID\n" +
                "GROUP BY AID\n" +
                "Order by SUM(Cost) DESC;";
        return ExecuteArrayPairInteger(query);
    }
    //Revise
    private ArrayList<Pair<Integer, Integer>> getNumberOfParkingByArea() {
        String query="SELECT  AID, COUNT(AID) as ParkingCount FROM ParkingArea as pa JOIN  CarParking as cp\n" +
                "ON pa.AID=cp.ParkingAreaID\n" +
                "GROUP BY AID\n" +
                "Order by COUNT(AID) DESC;";
        return ExecuteArrayPairInteger(query);
    }

    //Revise
    private ArrayList<Pair<Integer, Integer>> getNumberOfDistinctCarsByArea() {
        String query="SELECT  AID, COUNT(Distinct CID) as ParkingCount FROM ParkingArea as pa JOIN  CarParking as cp\n" +
                "ON pa.AID=cp.ParkingAreaID\n" +
                "GROUP BY AID\n" +
                "Order by COUNT(CID) DESC;";
        return ExecuteArrayPairInteger(query);
    }

    //Revise
    private void AddEmployee(int EID, String LastName, String FirstName, Date BirthDate, String StreetName, int Number, int door, String City) {
        try {
            CallableStatement cs=manager.conn.prepareCall(("{sp_AddMunicipalEmployee(?,?,?,?,?,?,?,?)}"));
            cs.setInt(1,EID);
            cs.setString(2,LastName);
            cs.setString(3,FirstName);
            cs.setDate(4,BirthDate);
            cs.setString(5,StreetName);
            cs.setInt(6,Number);
            cs.setInt(7,door);
            cs.setString(8,City);
            cs.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    //Revise
    private ArrayList<Pair<Integer, Integer>> ExecuteArrayPairInteger(String query){
        ArrayList<Pair<Integer,Integer>> output=new ArrayList<>();
        Statement s;
        try {
            s = manager.conn.createStatement();
            ResultSet rs=s.executeQuery(query);
            while (rs.next()){
                Pair<Integer,Integer> pair=new Pair<>(rs.getInt(1),rs.getInt(2));
                output.add(pair);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return output;

    }
}
