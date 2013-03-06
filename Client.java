import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.io.*;
import java.util.*;

 
public class Client {
	
	static void writeCardsToDB(Connection conn, int numberOfCards)
	{
		
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
				stmt.execute("SELECT insert_card(CAST ("+
				 cardnumber++ +" AS bigint), CAST ("+
				 daily[number_rnd10] +" AS numeric), CAST ( "+
				  monthly[number_rnd10] +" AS numeric), "+ false +
				  ", CAST("+ 5 +" AS SMALLINT), CAST( '"
				  +firstn.get(number_rndfn)+
				 " "+ lastn.get(number_rndln) + "' AS text));");
			
			} catch (SQLException e) {
				System.out.println("Error while executing Stored Procedure insert_card");
				e.printStackTrace();
				return;
			}
		}		
	}
	
		static void writeCountriesToDB(Connection conn)
	{
		//Load Countries from file
		String zeile = "";
		String[] parts = new String[4];
		
		try {
			FileReader fr = new FileReader("country_names_and_code_elements_txt.txt");
			BufferedReader br = new BufferedReader(fr);
			Boolean ok;
			
			Statement stmt = conn.createStatement();
			while( (zeile = br.readLine()) != null )
			{
				parts = zeile.split(";");
				//TODO
				//myApp.callProcedure("Insert_country", parts[1],parts[0],"");
				//myApp.callProcedure("Insert_country_specific", parts[1], parts[2],parts[3]);
				
				stmt.execute("SELECT insert_country(CAST ('"+
				parts[1] +"' AS text), CAST ('"+
				parts[0] +"' AS text), CAST(' ' AS text));");
				
				stmt.execute("SELECT insert_country_specific(CAST ('"+
				parts[1] +"' AS text), CAST ("+
				(ok = Integer.parseInt(parts[2]) == 1 ? true : false) +" AS BOOLEAN), CAST('"+
				parts[3] +"' AS numeric));");
				
				
			}
		br.close();
		fr.close();
		
		} catch (IOException e) {
			System.out.println("Dateilesefehler");
			e.printStackTrace();
			return;
		} catch (SQLException e) {
			System.out.println("Error while executing Stored Procedure insert_country or insert country specific");
			e.printStackTrace();
			return;
		}
	}
	
	static List<String> readNamesFromFile(String dateiname) 
	{
		List<String> names = new ArrayList<String>();
		String zeile = "";
		
		try {
			FileReader fr = new FileReader(dateiname);
			BufferedReader br = new BufferedReader(fr);

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
	
	
	static void transfers(Connection conn, int numberOfTransactions) {
			
			
		int numberOfCards = 1000000; // 10 Mio dauert ca. eine Stunde
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

		int numberOfTransfers = 1000000;
		int numberOfCards = 1000000; // 100.000 dauert ca. 15 min auf hdd und 1min 30sek auf ssd
		
		long startTime = System.nanoTime();
		writeCardsToDB(conn, numberOfCards);
		long endTime = System.nanoTime();
		double duration = (endTime - startTime)/1000000000.0; //in Sekunden
		System.out.println("Karten hinzufügen hat: "+ duration+" Sekunden gedauert.");
		System.out.println("Das sind: "+ numberOfCards/duration +" Karten pro Sekunde.");

		
		startTime = System.nanoTime();
		writeCountriesToDB(conn);
		endTime = System.nanoTime();
		duration = (endTime - startTime)/1000000000.0;
		System.out.println("Länder hinzufügen hat: "+ duration+" Sekunden gedauert.");
		

		startTime = System.nanoTime();
		transfers(conn, numberOfTransfers);
		endTime = System.nanoTime();
		duration = (endTime - startTime)/1000000000.0;
		System.out.println("Transfers haben "+ duration+" Sekunden gedauert.");
		System.out.println("Das sind: "+ numberOfTransfers/duration +" Transfers pro Sekunde.");
		
		System.out.printf("Datenbank vorbereitet \n\n");
		
	}
 
}
