import java.sql.*;
import java.util.Scanner;
import java.io.*;

public class DatabasesAssignment {
	public static Connection connectToDatabase(String user, String password) {
		System.out.println("----- PostgreSQL " 
				+ "JDBC Connection Testing -----");
		Connection connection = null;
		
		try {
			connection = DriverManager.getConnection(
					"jdbc:postgresql://teachdb.cs.rhul.ac.uk/CS2855/" + user,
					user, password);
		}	catch (SQLException e) {
			System.out.println("Connection Failed");
			e.printStackTrace();
	}
		return connection;

	}
	
	public static void createTable(Connection connection,
			String tableDescription) {
		Statement st = null;
		try {
			st = connection.createStatement();
			st.execute("CREATE TABLE " + tableDescription);
			st.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void createView(Connection connection, 
			String viewName, String viewDescription) {
		Statement st = null;
		try {
			st = connection.createStatement();
			st.execute("CREATE VIEW " + viewName + " AS " + viewDescription);
			st.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void dropTable(Connection connection, String table) {
		Statement st = null;
		try {
			st = connection.createStatement();
			st.execute("DROP TABLE IF EXISTS " + table);
			st.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void dropView(Connection connection, String view) {
		Statement st = null;
		try {
			st = connection.createStatement();
			st.execute("DROP VIEW IF EXISTS " + view);
			st.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static ResultSet executeSelect(Connection connection, String query) {
		Statement st = null;
		try {
			st = connection.createStatement();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}

		ResultSet rs = null;
		try {
			rs = st.executeQuery(query);
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}

		return rs;
	}
	
	public static int insertIntoTableFromFile(Connection connection,
			String table, String file) {

		BufferedReader br = null;
		int numRows = 0;
		try {
			Statement st = connection.createStatement();
			String sCurrentLine, brokenLine[], composedLine = "";
			br = new BufferedReader(new FileReader(file));

			while ((sCurrentLine = br.readLine()) != null) {
				brokenLine = sCurrentLine.split("\t");
				composedLine = "INSERT INTO " + table + " VALUES (";
				int i;
				for (i = 0; i < brokenLine.length - 1; i++) {
					composedLine += "'" + brokenLine[i] + "',";
				}
				composedLine += "'" + brokenLine[i] + "')";
				numRows = st.executeUpdate(composedLine);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)
					br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
		return numRows;
	}
	
	public static int insertIntoTableFromFileTwo(Connection connection,
			String table, String file) {
		BufferedReader br = null;
		int numRows = 0;
		try {
			Statement st = connection.createStatement();
			String sCurrentLine, brokenLine[], composedLine = "";
			br = new BufferedReader(new FileReader(file));

			while ((sCurrentLine = br.readLine()) != null) {
				brokenLine = sCurrentLine.split("\t");
				composedLine = "INSERT INTO " + table + " VALUES (";
				
				composedLine += "'" + brokenLine[0] + "',";
				composedLine += "'" + brokenLine[1] + "')";

				numRows = st.executeUpdate(composedLine);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)
					br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
		return numRows;
	}
	
	public static void main(String [] args) throws SQLException {
		Scanner scanner = new Scanner(System.in);
		System.out.println("Enter username:");
		String user = scanner.nextLine();
		System.out.println("Enter password:");
		String password = scanner.nextLine();
		
		Connection connection = connectToDatabase(user, password);
		
		if (connection != null) {
			System.out.println("You made it");
		}	else {
			System.out.println("Failed");
			return;
		}
		
		//drop views if they exist. These views are created in program later on.
		dropView(connection, "view3");
		dropView(connection, "view2");
		dropView(connection, "view");
		
		//drop table "topurls" if it exists.
		dropTable(connection, "topurls");
		createTable(connection, "topurls(rank int PRIMARY KEY, name varchar(128), tld1 varchar(128), tld2 varchar(128));");
		
		//insert data from file containing urls and separated by tabs from top level domanins.
		int rows = insertIntoTableFromFile(connection, "topurls", "src/TopURLs");
		
		//drop table "mappings" if it exists.
		dropTable(connection, "mappings");
		createTable(connection, "mappings(tld varchar(128) PRIMARY KEY, country varchar(128));");
		
		//insert data from file containing top level domains, separated by tabs from country.
		int rows2 = insertIntoTableFromFileTwo(connection, "mappings", "src/mapping");
		
		String query = "SELECT *"
				+ " FROM topurls"
				+ " ORDER BY rank ASC"
				+ " LIMIT 10;";
		
		ResultSet rs = executeSelect(connection, query);
		
		try {
			System.out.println("################# 1st Query ################");
			while (rs.next()) {
				System.out.println(rs.getString(1) +  "	" + rs.getString(2) + "	" +  rs.getString(3) + " " + rs.getString(4));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		rs.close();
		
		String query2 = "SELECT DISTINCT tld1, tld2, MIN(rank)"
				+ " FROM topurls"
				+ " GROUP BY tld1, tld2"
				+ " ORDER BY MIN(rank) ASC, tld1, tld2"
				+ " LIMIT 10;";
		
		ResultSet rs2 = executeSelect(connection, query2);
		
		try {
			System.out.println("################# 2nd Query ################");
			while (rs2.next()) {
				System.out.println(rs2.getString(1) + " " + rs2.getString(2));
			}
		}	catch (SQLException e) {
			e.printStackTrace();
		}
		rs2.close();
		
		
		createView(connection, "view", query2);
		
		String query3 = "SELECT country, min"
				+ " FROM mappings, view"
				+ " WHERE view.tld2 IS NULL AND"
				+ " mappings.tld = view.tld1"
				+ " UNION"
				+ " SELECT country, min"
				+ " FROM mappings, view"
				+ " WHERE view.tld2 IS NOT NULL AND "
				+ " mappings.tld = view.tld2"
				+ " ORDER BY min ASC;";
		
		ResultSet rs3 = executeSelect(connection, query3);
		
		try {
			System.out.println("################# 3rd Query ################");
			while (rs3.next()) {
				System.out.println(rs3.getString(1));
			}
		}	catch (SQLException e) {
			e.printStackTrace();
		}
		rs3.close();
		
		String query4 = "SELECT name, count(*)"
				+ " FROM url"
				+ " GROUP BY name"
				+ " HAVING count(*) > 1;";
		
		createView(connection, "view2", query4);
		
		String query5 = "SELECT DISTINCT name, MIN(rank)"
				+ " FROM url"
				+ " GROUP BY name"
				+ " ORDER BY MIN(rank) ASC;";
		
		createView(connection, "view3", query5);
		
		String query6 = "SELECT view3.name, view3.min"
				+ " FROM view3, view2"
				+ " WHERE view3.name = view2.name"
				+ " ORDER BY view3.min"
				+ " LIMIT 10;";
		
		ResultSet rs4 = executeSelect(connection, query6);
		try {
			System.out.println("################# 4th Query ################");
			while (rs4.next()) {
				System.out.println(rs4.getString(1));
			}
		}	catch (SQLException e) {
			e.printStackTrace();
		}
		rs4.close();
		
	}
	
	
}
