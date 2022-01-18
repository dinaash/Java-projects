import java.sql.Date;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.bson.Document;
import org.bson.types.ObjectId;

import com.mongodb.client.model.InsertManyOptions;

public class requestInsert implements Runnable{
	
	private final BlockingQueue<Long[]> queue;
	private final int idStart;
	private final int idEnd;
	
	requestInsert(BlockingQueue<Long[]> q, int first, int last) { queue = q; idStart=first; idEnd=last;}
	
	public void run() {
		
		try {
			queue.put(produceRequestInsert (idStart,idEnd));
			
		}
		catch (InterruptedException ex) {
			Thread.currentThread().interrupt();  // set interrupt flag
			ex.printStackTrace();
        }
	}// end run
	
	public Long[] produceRequestInsert (int idStart, int idEnd) {
		long requestType = 1;
		long requestExecutionTime = 0;
		
		
		while (requestExecutionTime == 0) {
			
			List<Document> citizensList = new ArrayList<>();
			for (int id = idStart; id <idEnd; id++) {
				
				Document citizen = new Document("_id", new ObjectId());
				citizen.append("ID", (int)(Math.random()*100000));
				citizen.append("name", getAlphaString((int)(3 + (5 * Math.random()))));
				citizen.append("dateBirth",randomDateOfBirth ());
				citizen.append("gender",(id%2 == 0) ? 'f' : 'm');
				citizen.append("city", getAlphaString((int)(3 + (5 * Math.random()))));
				citizen.append("zipCode", getZipCode());
				citizen.append("streetName", getAlphaString((int)(3 + (5 * Math.random()))));
				citizen.append("streetNumber",(int)(Math.random()*100));
				citizen.append("parentNames", getAlphaString((int)(3 + (5 * Math.random()))));
				citizen.append("organisation",(int)(Math.random()*1000));
				
				citizensList.add(citizen);
				
			}//end for loop
			
			long startTime = System.currentTimeMillis();
			mongoConnection.citizens.insertMany(citizensList, new InsertManyOptions().ordered(false));		
			long endTime = System.currentTimeMillis();
			requestExecutionTime = endTime-startTime;
		}
		
		
		
		Long[] requestStat = {requestType,requestExecutionTime};
		return requestStat;
	}//end method produceRequest
	
	static String getAlphaString(int n) { 
		
		String AlphaNumericString = "abcdefghijklmnopqrstuvxyz"; 
		StringBuilder sb = new StringBuilder(n); 

		for (int i = 0; i < n; i++) { 
			int index = (int)(AlphaNumericString.length() * Math.random()); 
			sb.append(AlphaNumericString .charAt(index)); 
			} 

		return sb.toString(); 
		}//end method get..String
	
	static String getZipCode() { 
		
		String AlphaNumericString = "ABCDEFGHIGKLMNOPQRSTUVW0123456"; 
		StringBuilder sb = new StringBuilder(6); 

		for (int i = 0; i < 6; i++) { 
			int index = (int)(AlphaNumericString.length() * Math.random()); 
			sb.append(AlphaNumericString .charAt(index)); 
			} 

		return sb.toString(); 
		}//end method get..String
	
	
	public static long randomDateOfBirth () {
		
		int year = 1900 + (int)(Math.random()*(2020-1900));
		int month = 1 + (int)(Math.random()*(12-1));
		
		int day = 1 + (int)(Math.random()*(31-1));
		long bdate = 0;
		try {bdate = LocalDate.of(year, month, day).toEpochDay();}
		catch (Exception e){day = 1 + (int)(Math.random()*(28-1));bdate = LocalDate.of(year, month, day).toEpochDay();}
		return bdate;
		
	}// end method

}//end class
