import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import com.eclipsesource.json.JsonObject;

public class SpoolIt {
	final static String _APP_VERSION = "0.2.1";
	
	private static int FETCHSIZE = 1024;
	private static JsonObject jsonObject;
	
	//default constructor
	public SpoolIt() {}

	//main program
	public static void main(String[] args) throws Exception {
		String parfile = "";
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		ResultSetMetaData rsmd = null;
		int n = 0;
		long oldTime, curTime, sumTime, avgTime = 0;
		int rows = 0;
		try {
			parfile = args[0];
		} catch(Exception exc) {
			System.out.println("Please use SpoolIt [configuration file]");
			System.exit(1);
		}

		try {
			System.out.println("------------------------------------------------------------");
			System.out.println("spoolIt Version " + _APP_VERSION);
			System.out.println("------------------------------------------------------------");
			System.out.println("Report bugs to : syahreza.octadian@gmail.com");
			//record start time
			oldTime = System.currentTimeMillis();

			//read parameter file
			try {
				/*
				 * Example of configuration
				 *  
					{
					"DATABASE": "oracle",
					"INSTANCE": "your-database-instance-or-sid",
					"HOST": "your-database-ip-address",
					"PORT": "1521",
					"USER": "your-database-user",
					"PASSWORD": "your-database-password",
					"QUERY" : "SELECT * FROM DUAL WHERE ROWNUM <= 3",
					"OUTFILE": "/your/path/filename.dat",
					"DELIMITER": "|"
					}
				*
				*/
				BufferedReader in = new BufferedReader(new FileReader(parfile));
				jsonObject = JsonObject.readFrom( in );
				jsonObject.add("CONNECTION_STRING", "");
			} catch (IOException e) {
			}

			System.out.print("\n-------------------- Configuration -------------------------\n");
			System.out.println("Database product     : " + jsonObject.get("DATABASE").asString() );
			System.out.println("Database Instance    : " + jsonObject.get("INSTANCE").asString() );
			System.out.println("Database Port	     : " + jsonObject.get("PORT").asString() );
			System.out.println("Database User ID     : " + jsonObject.get("USER").asString() );
			System.out.println("SQL Query for export : " + jsonObject.get("QUERY").asString() );

			System.out.println("\n-------------------- Dump File -----------------------------");
			System.out.println("Path : " + jsonObject.get("OUTFILE").asString() );
			System.out.println("Type : Variable length separated by delimeter \"" + jsonObject.get("DELIMITER").asString() + "\"");
			System.out.println("Rows Fetch Size : " + FETCHSIZE);

			//load driver
			LoadDriver ld = new LoadDriver();
			ld.loadNow(jsonObject.get("DATABASE").asString());

			//create connection to database
			if(jsonObject.get("DATABASE").asString().equals("mysql")){
				jsonObject.set("CONNECTION_STRING", "jdbc:" + jsonObject.get("DATABASE").asString() + 
						"://" + jsonObject.get("HOST").asString() + "/" + jsonObject.get("INSTANCE").asString() + 
						"?user=" + jsonObject.get("USER").asString() + 
						"&password=" + jsonObject.get("PASSWORD").asString() );
				//System.out.println("Connection string : " + jsonObject.get("CONNECTION_STRING").asString() );

				conn = DriverManager.getConnection(jsonObject.get("CONNECTION_STRING").asString());
			}else if(jsonObject.get("DATABASE").asString().equals("oracle")) {
				/*
				 * Connection string using service name
						jdbc:oracle:thin:@//oracle.hostserver2.mydomain.ca:1522/ABCD
				 *
				 * Connection string using tnsname
						jdbc:oracle:thin:@oracle.hostserver2.mydomain.ca:1522:ABCD
				 *
				*/
				//jsonObject.set("CONNECTION_STRING", "jdbc:oracle:thin:@" + jsonObject.get("HOST").asString() + 
				//		":" + jsonObject.get("PORT").asString() + ":" + jsonObject.get("INSTANCE").asString() );
				jsonObject.set("CONNECTION_STRING", "jdbc:oracle:thin:@//" + jsonObject.get("HOST").asString() + 
						":" + jsonObject.get("PORT").asString() + "/" + jsonObject.get("INSTANCE").asString() );

				//System.out.println("Connection string : " + jsonObject.get("CONNECTION_STRING").asString() );
				conn = DriverManager.getConnection(jsonObject.get("CONNECTION_STRING").asString(), 
						jsonObject.get("USER").asString(), 
						jsonObject.get("PASSWORD").asString() );
			} else if (jsonObject.get("DATABASE").asString().equals("teradata")) {
                conn = DriverManager.getConnection("jdbc:teradata://" + jsonObject.get("HOST").asString() + 
                		"/DATABASE=" + jsonObject.get("INSTANCE").asString() + ",TMODE=ANSI,CHARSET=UTF8,TYPE=FASTEXPORT", jsonObject.get("USER").asString(), jsonObject.get("PASSWORD").asString());
			} else if (jsonObject.get("DATABASE").asString().equals("impala")) {
                //jdbc:hive2://myhost.example.com:21050/test_db;auth=noSasl
				conn = DriverManager.getConnection("jdbc:hive2://" + jsonObject.get("HOST").asString() + 
						":" + jsonObject.get("PORT").asString() +
                		"/" + jsonObject.get("INSTANCE").asString() + 
                		";auth=noSasl"
                		);
			}

			//Implementation
			try {
			    PreparedStatement ps = conn.prepareStatement(jsonObject.get("QUERY").asString());
				//stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
				//stmt.setFetchSize(FETCHSIZE);
				System.out.print("\nExecuting query...");
				//change fetch size on result set
				//rs.setFetchSize(FETCHSIZE);
				//rs.setFetchDirection(ResultSet.FETCH_FORWARD);
				//rs = stmt.executeQuery(vQuery);
				rs = ps.executeQuery();
				if(rs != null) {
					System.out.print("Done\n");
					//change fetch size on result set
					//rs = stmt.getResultSet();
					//rs.setFetchSize(FETCHSIZE);
					rsmd = rs.getMetaData();
					n = rsmd.getColumnCount();
					//fetch data and save to file
					BufferedWriter out = new BufferedWriter(new FileWriter(jsonObject.get("OUTFILE").asString()));
					String tmpStr = "";
					System.out.print("Exporting data to " + jsonObject.get("OUTFILE").asString() + "\n");
					while(rs.next()) {
						try {
							tmpStr = "";

							for(int index=1; index<=n; index++) {
								if(index != n)
									tmpStr = tmpStr + rs.getString(index) + jsonObject.get("DELIMITER").asString();
								else
									tmpStr = tmpStr + rs.getString(index) + "\n";
							}
							//tmpStr = rs.getString(1) + "\n";
							out.write(tmpStr);
							rows++;
						} catch (IOException e) {
						}
						//sleep milisecond
					}
					//close dump file
					out.close();
					System.out.println("Export succesfully");
				}
			} finally {
				try {
					if(rs != null) {
						rs.close();
					}
					rs = null;
				} catch (SQLException sqlEx) { }
			}

			try {
				//release connection
				conn.close();
			} catch(SQLException sqlEx) {}
			//summary of time execution
			curTime = System.currentTimeMillis();
			sumTime = (curTime - oldTime);

			System.out.println("\n-------------------- Performance ---------------------------");
			System.out.println("Total rows exported : " + rows + " rows");
			System.out.println("Overall dump speed  : " + sumTime + " miliseconds");
		} catch(SQLException ex) {
			System.out.println("SQLException : " + ex.getMessage());
		}
	}
}

//class to load driver
class LoadDriver {
	//default constructor
	public LoadDriver() {}

	public void loadNow(String dbType) {
		try {
			// The newInstance() call is a work around for some
			// broken Java implementations
			if(dbType.equals("mysql")) {
				Class.forName("com.mysql.jdbc.Driver").newInstance();
			} else if(dbType.equals("oracle")) {
				Class.forName("oracle.jdbc.driver.OracleDriver").newInstance();
			} else if (dbType.equals("teradata")) {
			     Class.forName("com.teradata.jdbc.TeraDriver");
			} else if (dbType.equals("impala")) {
			     Class.forName("org.apache.hive.jdbc.HiveDriver");
			}
		} catch (Exception ex) {
			// handle the error
		}
	}
}
