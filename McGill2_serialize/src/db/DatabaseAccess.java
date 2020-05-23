package db;
import java.sql.*;
import java.util.ArrayList;
import redis.clients.jedis.Jedis;
import org.json.JSONObject;


public class DatabaseAccess extends Thread {
	Statement statement;
	Jedis jedis;
	
	static int numRequests = 100;
	
	static int percentRead = 50;
	static int percentInsert = 50;
	// percentUpdate is simply remaining amount

	// Credentials
	static String username = "sever";
	static String password = "1234";
	
	// JDBC URLs
	static String urlPSQL = "jdbc:postgresql://192.168.2.24:5432/mcgill";
	static String urlMySQL = "jdbc:mysql://192.168.0.108:3306/mcgill?allowPublicKeyRetrieval=true&useSSL=false";
	static String urlMonetDB = "jdbc:monetdb://192.168.0.108:1337/mcgill";
	static String urlDB2 = "jdbc:db2://192.168.0.108:6969/mcgill";

	public void run() {
		try {
			Connection con = DriverManager.getConnection(urlPSQL, username, password);
			statement = con.createStatement();

			jedis = Main.jedisManager.getJedis();
			
			for (int i = 0; i < numRequests; i++) {
				// Randomly choose an operation based on % probabilities
				int choice = (int) (Math.random() * 100); // Generate random number 0-99

				if (choice < percentRead) {
					executeRead();
				} else if (choice < percentRead + percentInsert) {
					executeInsert();
				} else {
					executeUpdate();
				}
			}

			statement.close();
			con.close();
			Main.jedisManager.returnJedis(jedis);
			Main.threads --;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// Randomly chooses read instruction to execute from Queries
	void executeRead() {
		int index = (int) ((Math.random() * 18)); // Generate number 0-17
		String readSQL = Queries.readQueries[index]; // Get query to run
		
		try {
			if (jedis.lrange(Integer.toString(readSQL.hashCode()), 0, -1).size() == 0) {
				java.sql.ResultSet rs = statement.executeQuery(readSQL);
				ArrayList<String> jsonrs = resultSetToJsonObject(rs);
				for(int i = 0 ; i < jsonrs.size();i++) {
					jedis.rpush(Integer.toString(readSQL.hashCode()),jsonrs.get(i) );
				}
				
				System.out.println("Not found in cache.");
				System.out.println(jsonrs);
			}else {
				ArrayList<String> ans = (ArrayList<String>)jedis.lrange(Integer.toString(readSQL.hashCode()), 0, -1);
				System.out.println("Retrieved from cache.");
				System.out.println(ans.toString());
			}
			
			
		} catch (SQLException e) {
			String sqlMessage = e.getMessage();
			System.out.println(sqlMessage);
		}
	}

	// Randomly chooses insert instruction to execute
	void executeInsert() {
		String ccode = getAlphaNumericString(10); // Generate random string of length 10
		int credits = (int) ((Math.random() * 100)); // Generate random number 0-5
		
		String insertSQL = "INSERT INTO course VALUES (\'" + ccode + "\', " + credits + ", \'dept\');";
		
		try {
			// Execute query
			statement.executeUpdate(insertSQL);
		} catch (SQLException e) {
			String sqlMessage = e.getMessage();
			System.out.println(sqlMessage);
		}
	}

	// Randomly chooses update instruction to execute
	void executeUpdate() {
		int credits = (int) ((Math.random() * 100)); // Generate random number 0-5
		
		String updateSQL = "UPDATE course SET credits = " + credits + " WHERE ccode = \'math-241\';";
		
		try {
			// Execute query
			statement.executeUpdate(updateSQL);
		} catch (SQLException e) {
			String sqlMessage = e.getMessage();
			System.out.println(sqlMessage);
		}
	}
	
	
	// Generate random string of length n
	static String getAlphaNumericString(int n) 
    { 
  
        // chose a Character random from this String 
        String AlphaNumericString = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                    + "0123456789"
                                    + "abcdefghijklmnopqrstuvxyz"; 
  
        // create StringBuffer size of AlphaNumericString 
        StringBuilder sb = new StringBuilder(n); 
  
        for (int i = 0; i < n; i++) { 
  
            // generate a random number between 
            // 0 to AlphaNumericString variable length 
            int index 
                = (int)(AlphaNumericString.length() 
                        * Math.random()); 
  
            // add Character one by one in end of sb 
            sb.append(AlphaNumericString 
                          .charAt(index)); 
        } 
        return sb.toString(); 
    } 

    public static ArrayList<String> resultSetToJsonObject(ResultSet rs) throws SQLException{ 
       // json Obj
    	ArrayList<String> ans = new ArrayList<String>();
    	ResultSetMetaData metaData = rs.getMetaData(); 
    	int columnCount = metaData.getColumnCount(); 
        if (rs.next()) { 
        	JSONObject jsonObj = new JSONObject();    
            for (int i = 1; i <= columnCount; i++) { 
                String columnName =metaData.getColumnLabel(i); 
                String value = rs.getString(columnName); 
                jsonObj.put(columnName, value); 
            } 
            String str = jsonObj.toString();
            ans.add(str);
        }
        return ans; 
    }
}