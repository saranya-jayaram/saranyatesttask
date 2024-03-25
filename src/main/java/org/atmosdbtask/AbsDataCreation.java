package org.atmosdbtask;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;

import static org.atmosdbtask.DBConnect.getRecordsBetweenInt;

public class AbsDataCreation {
    private static String jdbcUrl = "jdbc:sqlite:atmos.db";
    public static void main(String[] args) throws SQLException, IOException {
        Connection connection;
        String dataBaseName = "atmos";
        createNewDatabase(dataBaseName);
        String jdbcUrl = String.format("jdbc:sqlite:%s.db",dataBaseName);
        createAbsTable("abs");
        insertDataIntoTable();

        {
            try {
                connection = DriverManager.getConnection(jdbcUrl);
                String sql = getRecordsBetweenInt("time", 3855, 215348);
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(sql);
                while (rs.next()) {
                    System.out.println(rs.getString("module")
                            + "\t" + rs.getInt("time")
                            + "\t" + rs.getDouble("temp"));
                }
            } catch (SQLException e) {
                System.out.println("Error Connecting to SQLite DB");
                e.printStackTrace();
            }
        }
    }
    private static void createNewDatabase(String fileName) {

        String url = "jdbc:sqlite:" + fileName;

        try (Connection conn = DriverManager.getConnection(url)) {
            if (conn != null) {
                DatabaseMetaData meta = conn.getMetaData();
                System.out.println("The driver name is " + meta.getDriverName());
                System.out.println("A new database has been created.");
            }

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    private static void createAbsTable(String tableName){
        String sql = "CREATE TABLE abs(\"id\" integer, \"module\" text, \"time\" int, \"press\" real, \"temp\" real, \"alt\" real);";
        try (Connection conn = DriverManager.getConnection(jdbcUrl);
             Statement stmt = conn.createStatement()) {
            // create a new table
            stmt.execute(sql);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    private static void insertDataIntoTable() throws IOException, SQLException {
        File file = new File("abssheet.csv");
        String absolutePath = file.getAbsolutePath();
        BufferedReader br = new BufferedReader(new FileReader(absolutePath));
        String line;
        Connection connection;
        connection = DriverManager.getConnection(jdbcUrl);
        String insertQuery = "INSERT INTO abs "+"(id, module, time,press,temp,alt) VALUES (?, ?, ?,?,?,?)";
        PreparedStatement preparedStatement = connection.prepareStatement(insertQuery);
        while ( (line=br.readLine()) != null)
        {
            String[] values = line.split(",");
            preparedStatement.setInt(1,Integer.parseInt(values[0]));
            preparedStatement.setString(2,values[1]);
            preparedStatement.setInt(3,Integer.parseInt(values[2]));
            preparedStatement.setDouble(4,Double.parseDouble(values[3]));
            preparedStatement.setDouble(5,Double.parseDouble(values[4]));
            preparedStatement.setDouble(6,Double.parseDouble(values[5]));
            preparedStatement.executeUpdate();
        }
        br.close();
    }
}
