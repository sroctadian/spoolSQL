
/*
    $Log: SpoolIt.java,v $
    Revision 1.1  2013/01/04 02:46:45  sroctadian
    Initial revision

*/

import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.io.*;

public class SpoolIt {
	//variable or behaviour
	private static String vDb = "";
	private static String vUser = "";
	private static String vPassword = "";
	private static String vHost = "";
	private static String vURL = "";
	private static String vQuery = "";
	private static String vDbType = "";
	private static String vOutFile = "";
	private static String vDelim = "|";
	private static int FETCHSIZE = 1024;

	//default constructor
	public SpoolIt() {}

	//method
	public static void getParameters(String parFile) {
		String[] values;
		String str;
		String tmp;

		try {
			/* sample parameter
			HOST=localhost
			USER=syahreza
			*/
			BufferedReader in = new BufferedReader(new FileReader(parFile));
			//read parameter file
			System.out.println("Configuration file");
			while ((str = in.readLine()) != null) {
				values = str.split("=");
				tmp = values[0];
				try {
					if(tmp.equals("DATABASE")) {
						vDbType = values[1];
						//System.out.println("Database	\t: " + vDbType);
					}
					if(tmp.equals("INSTANCE")) {
						vDb = values[1];
						//System.out.println("Instanse	\t: " + vDb);
					}
					if(tmp.equals("HOST")) {
						vHost = values[1];
						//System.out.println("Host		\t: " + vHost);
					}
					if(tmp.equals("USER")) {
						vUser = values[1];
						//System.out.println("User Name	\t: " + vUser);
					}
					if(tmp.equals("PASSWORD")) {
						vPassword = values[1];
						//System.out.println("Password	\t: " + vPassword);
					}
					if(tmp.equals("QUERY")) {
						vQuery = values[1];
						//System.out.println("Query		\t: " + vQuery);
					}
					if(tmp.equals("OUTFILE")) {
						vOutFile = values[1];
						//System.out.println("OutFile		\t: " + vOutFile);
					}
					if(tmp.equals("DELIMITER")) {
						vDelim = values[1];
						//System.out.println("DELIMITER		\t: " + vDelim);
					}
					if(tmp.equals("FETCHSIZE")) {
						FETCHSIZE = Integer.valueOf(values[1]);
						//System.out.println("DELIMITER		\t: " + vDelim);
					}
				} catch(ArrayIndexOutOfBoundsException ex) {
					//use default setting
					continue;
				}
			}
			in.close();
		} catch (IOException e) {
		}
	}

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
			System.out.println("                Spool IT Version 0.1.0");
			System.out.println("------------------------------------------------------------");
			//record start time
			oldTime = System.currentTimeMillis();
			//read parameter file
			getParameters(parfile);

			System.out.print("\n-------------------- Configuration -------------------------\n");
			System.out.println("Database product     : " + vDbType);
			System.out.println("Database Instance    : " + vDb);
			System.out.println("Database User ID     : " + vUser);
			System.out.println("SQL Query for export : " + vQuery);

			System.out.println("\n-------------------- Dump File -----------------------------");
			System.out.println("Path : " +vOutFile );
			System.out.println("Type : Variable length separated by delimeter \"" + vDelim + "\"");
			System.out.println("Rows Fetch Size : " + FETCHSIZE);

			//load driver
			LoadDriver ld = new LoadDriver();
			ld.loadNow(vDbType);

			//create connection to database
			if(vDbType.equals("mysql")){
				vURL = "jdbc:" + vDbType + "://" + vHost + "/" + vDb + "?user=" + vUser + "&password=" + vPassword;
				conn = DriverManager.getConnection(vURL);
			}else if(vDbType.equals("oracle")) {
				//vURL = "jdbc:oracle:thin:@(description=(address=(host=" + vHost + ") (protocol=tcp)(port=1521))(connect_data=(sid=" + vDb + ")))";
				//conn = DriverManager.getConnection(vURL, vUser, vPassword);
				conn = DriverManager.getConnection( "jdbc:oracle:thin:@" + vHost + ":1521:" + vDb, vUser, vPassword);
			} else if (vDbType.equals("teradata")) {
                conn = DriverManager.getConnection("jdbc:teradata://" + vHost + "/DATABASE=" + vDb + ",TMODE=ANSI,CHARSET=UTF8,TYPE=FASTEXPORT", vUser, vPassword);
			}

			//Implementation
			try {
			    PreparedStatement ps = conn.prepareStatement(vQuery);
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
					BufferedWriter out = new BufferedWriter(new FileWriter(vOutFile));
					String tmpStr = "";
					System.out.print("Exporting data to " + vOutFile + "\n");
					while(rs.next()) {
						try {
							tmpStr = "";

							for(int index=1; index<=n; index++) {
								if(index != n)
									tmpStr = tmpStr + rs.getString(index) + vDelim;
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
//					if(stmt != null) {
//						stmt.close();
//					}
//					stmt = null;
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
			//System.out.println("Started  : Wed Sep  5 01:10:54 2007");
			//System.out.println("Finished : Wed Sep  5 01:10:55 2007");
			//create log file

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
			}
		} catch (Exception ex) {
			// handle the error
		}
	}
}



