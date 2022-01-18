
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.concurrent.BlockingQueue;


public class insertRequest implements Runnable{
	
	private final BlockingQueue<Long[]> queue;
	private final int idStart;
	private final int idEnd;
	
	
	insertRequest(BlockingQueue<Long[]> q, int first, int last) { queue = q; idStart=first; idEnd=last;}
	
	public void run() {
		
		try {
			queue.put(produceInsertRequest (idStart,idEnd));
			
		}
		catch (InterruptedException ex) {
			Thread.currentThread().interrupt();  // set interrupt flag
			ex.printStackTrace();
        }
		
	}// end run
	
	public Long[] produceInsertRequest (int idStart, int idEnd) {
		long requestType = 1;
		long requestExecutionTime = 0;
		
		while (requestExecutionTime == 0) {
			try {
				
				
					Connection connection = mysqlConnection.connectionPoolW
					          .remove(mysqlConnection.connectionPoolW.size() - 1);
					mysqlConnection.usedConnections.add(connection);
					
					PreparedStatement statement;
					
					String query = " insert into citizens (name, dateBirth, gender, city, zipCode, streetName, streetNumber, parentNames,organisation)"
					        + " values (?,?, ?, ?, ?, ?, ?, ?, ?)";
					statement = connection.prepareStatement(query);
					System.out.println("INSERT statement prepared");
					for (int id = idStart; id <=idEnd; id++) {
						//statement.setInt (1, id);
						statement.setString (1, getAlphaString((int)(3 + (5 * Math.random()))));
						statement.setDate  (2, randomDateOfBirth ());
						statement.setString (3, (id%2 == 0) ? "f" : "m");
						statement.setString (4, getAlphaString((int)(3 + (5 * Math.random()))));
						statement.setString (5, getZipCode());
						statement.setString (6, getAlphaString((int)(3 + (5 * Math.random()))));
						statement.setInt (7, (int)(Math.random()*100));
						statement.setString (8, getAlphaString((int)(3 + (5 * Math.random()))));
						statement.setInt (9, (int)(Math.random()*1000)); 
					    statement.addBatch();
					}
					long startTime = System.currentTimeMillis();
					statement.executeBatch().toString();
					long endTime = System.currentTimeMillis();
					statement.close();
					requestExecutionTime = (long)(endTime-startTime);
					mysqlConnection.connectionPoolW.add(connection);
					mysqlConnection.usedConnections.remove(connection);
				
				} catch (SQLException e) {
					e.printStackTrace();
					requestExecutionTime = 0;
				}
				catch (ArrayIndexOutOfBoundsException e) {
				e.printStackTrace();
				requestExecutionTime = 0;
				}//end try catch
		}//end while
		Long[] requestStat = {requestType,requestExecutionTime};
		return requestStat;
		
	}//end method 
	
	
	static String getAlphaString(int n) { 
		
		String AlphaNumericString = "abcdefghijklmnopqrstuvxyz"; 
		StringBuilder sb = new StringBuilder(n); 

		for (int i = 0; i < n; i++) { 
			int index = (int)(AlphaNumericString.length() * Math.random()); 
			sb.append(AlphaNumericString .charAt(index)); 
			} 

		return sb.toString(); 
		}//end method 
	
	static String getZipCode() { 
		
		String AlphaNumericString = "ABCDEFGHIGKLMNOPQRSTUVW0123456"; 
		StringBuilder sb = new StringBuilder(6); 

		for (int i = 0; i < 6; i++) { 
			int index = (int)(AlphaNumericString.length() * Math.random()); 
			sb.append(AlphaNumericString .charAt(index)); 
			} 

		return sb.toString(); 
		}//end method 
	
	
	public static Date randomDateOfBirth () {
		
		int year = 1900 + (int)(Math.random()*(2020-1900));
		int month = 1 + (int)(Math.random()*(12-1));
		
		int day = 1 + (int)(Math.random()*(31-1));
		Date bdate = null;
		try {bdate = Date.valueOf(LocalDate.of(year, month, day));}
		catch (Exception e){day = 1 + (int)(Math.random()*(28-1));bdate = Date.valueOf(LocalDate.of(year, month, day));}
		return bdate;
		
	}// end method

}//end class
