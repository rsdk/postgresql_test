import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.io.*;
import java.util.*;
 
public class Client {
	
	static void writeCardsToDB(Connection conn)
	{
	int numberOfCards = 100000; // 10 Mio dauert ca.
	long cardnumber_start = 1111222233330000L;
	long cardnumber = cardnumber_start;
	int[] daily = {100, 200, 500, 1000, 2000, 5000, 10000, 20000, 100000, 1000000};
	int[] monthly = {1000, 2000, 5000, 10000, 20000, 50000, 100000, 200000, 1000000, 10000000};
	int number_rnd10 = 0;
	int number_rndfn = 0;
	int number_rndln = 0;
	List<String> firstn = readNamesFromFile("baby-names.csv");
	List<String> lastn = readNamesFromFile("surnames.csv");
	
	for (int i = 0; i < numberOfCards; i++){
		if ( i % 100 == 0 ) {
			System.out.println("Number of Cards: \t" + i);
		}
		number_rnd10 = (int) (Math.random()*10);  //random number from 0 till 9
		number_rndfn = (int) (Math.random()*firstn.size());  //random number for namearray
		number_rndln = (int) (Math.random()*lastn.size());   //random number for namearray
		
		try {
		
			Statement stmt = conn.createStatement();
			stmt.execute("SELECT insert_card(CAST ("+ cardnumber++ +" AS bigint), CAST ("+
			 daily[number_rnd10] +" AS numeric), CAST ( "+ monthly[number_rnd10] +
			 " AS numeric), "+ true +", CAST("+ 5 +" AS SMALLINT), CAST( '"+firstn.get(number_rndfn)+
			 " "+ lastn.get(number_rndln) + "' AS text));");
		
		} catch (SQLException e) {
 
			System.out.println("Error while executing Stored Procedure insert_card");
			e.printStackTrace();
			return;
		}
		
	}		
	
		
	}
	
	static List<String> readNamesFromFile(String dateiname) 
	{
		List<String> names = new ArrayList<String>();
		String zeile = "";
		FileReader fr = null;
		BufferedReader br = null;
		
		try {
			fr = new FileReader(dateiname);
			br = new BufferedReader(fr);

			while( (zeile = br.readLine()) != null )
			{
				names.add(zeile);
			}
			br.close();
			fr.close();
		
		} catch (IOException e) {
			System.out.println("Dateilesefehler");
			e.printStackTrace();
			System.exit(-1);
		}
			
		return names;
	}
	
	public static void main(String[] argv) {
 
		System.out.println("-------- PostgreSQL "
				+ "JDBC Connection Testing ------------");
 
		try {
 
			Class.forName("org.postgresql.Driver");
 
		} catch (ClassNotFoundException e) {
 
			System.out.println("Where is your PostgreSQL JDBC Driver? "
					+ "Include in your library path!");
			e.printStackTrace();
			return;
 
		}
 
		System.out.println("PostgreSQL JDBC Driver Registered!");
 
		Connection conn = null;
 
		try {
 
			conn = DriverManager.getConnection(
					"jdbc:postgresql://127.0.0.1:5432/postgres", "postgres",
					"test");
 
		} catch (SQLException e) {
 
			System.out.println("Connection Failed! Check output console");
			e.printStackTrace();
			return;
 
		}
 
		if (conn != null) {
			System.out.println("Hello World!");
		} else {
			System.out.println("Failed to make connection!");
		}
		
		
		writeCardsToDB(conn);

		//TODO länder
		System.out.printf("Datenbank vorbereitet \n\n");
		
	}
 
}
