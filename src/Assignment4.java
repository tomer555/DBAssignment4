import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.Calendar;
import java.text.SimpleDateFormat;
import java.io.File;

import javafx.util.Pair;


import java.util.ArrayList;



public class Assignment4 {
    private DatabaseManager manager;
    private Assignment4() {
        //Establish connection to DB2019_Ass2
        this.manager=new DatabaseManagerMSSQLServer();
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

    private void updateEmployeeSalaries(double percentage) {

    }


    public void updateAllProjectsBudget(double percentage) {

    }

    //Revise
    private double getEmployeeTotalSalary() {
        String query="SELECT SUM(SalaryPerDay) FROM ConstructorEmployee";
        PreparedStatement statement;
        double output=-1;
        try {
            statement = manager.conn.prepareStatement(query);
            ResultSet result = statement.executeQuery();
            String sum = result.getString(1);
            output= Double.parseDouble(sum);
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
            String sum = result.getString(1);
            output= Integer.parseInt(sum);
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
        return 0;
    }

    private ArrayList<Pair<Integer, Integer>> getMostProfitableParkingAreas() {
        return null;
    }

    private ArrayList<Pair<Integer, Integer>> getNumberOfParkingByArea() {
        return null;
    }


    private ArrayList<Pair<Integer, Integer>> getNumberOfDistinctCarsByArea() {
        return null;
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
}
