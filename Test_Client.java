import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.io.*;
import java.util.*;

 
public class Test_Client {
	
		static void transfers(Connection conn, int numberOfTransactions) {
			
			
		int numberOfCards = 10000; // 10 Mio dauert ca. eine Stunde
		long cardnumber_start = 1111222233330000L;

		long number_rndcard = 0;
		long amount = 0;
		String country_code = "DE";
		double lat_neu = 48.4;
		double long_neu = 13.9;
		
		long rnd_cardnumber = 0L;
		
		for (int k = 0; k < numberOfTransactions; k++) {
			amount = (int) (Math.random()*10);
			rnd_cardnumber = cardnumber_start+((int) (Math.random()*numberOfCards));
			if ( k % 1000 == 0 ) {
				System.out.println("Transfer-number: " + k);
			}
			String zweck = "TEST " + k;
			
			try {
					Statement stmt = conn.createStatement();
					stmt.execute("SELECT new_transfer(CAST ("+
					 rnd_cardnumber +" AS bigint), CAST ("+
					 amount +" AS numeric), CAST ( "+
					  48.2 +" AS double precision), CAST ("+
					  13.0 +" AS double precision), CAST("+ "'DE'" +" AS text), CAST( '"+
					 zweck + "' AS text));");
				
				} catch (SQLException e) {
					System.out.println("Error while executing Stored Procedure new_transfer");
					e.printStackTrace();
					return;
				}	
			}
		
		
		}
	
		public static void main(String[] argv) {
 
		System.out.println("-------- PostgreSQL "
				+ "JDBC Connection Testing ------------");
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
			System.out.println("Connection OK.");
		} else {
			System.out.println("Failed to make connection!");
		}
		
		int numberOfTransfers = 10000;
		
		long startTime = System.nanoTime();
		transfers(conn, numberOfTransfers);
		long endTime = System.nanoTime();
		double duration = (endTime - startTime)/1000000000.0; //in Sekunden
		System.out.println("Karten hinzufügen hat: "+ duration+" Sekunden gedauert.");
		System.out.println("Das sind: "+ numberOfTransfers/duration +" Karten pro Sekunde.");
		
	}
 
}
