import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.client.FindIterable;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;

public class requestUpdate implements Runnable {

	private final BlockingQueue<Long[]> queue;
	
	requestUpdate(BlockingQueue<Long[]> q) {  queue = q;}
	
	public void run() {
		
		try {
			queue.put(produceRequestUpdate ());
			
		}
		catch (InterruptedException ex) {
			Thread.currentThread().interrupt();  // set interrupt flag
			ex.printStackTrace();
        }
	}// end run
	
	public Long[] produceRequestUpdate () {
		
		long requestType = 5;
		long requestExecutionTime = 0;
		
		while (requestExecutionTime == 0) {
			Bson filter = Filters.eq("ID", (int)(Math.random()*100000));	
			
			
			Bson cityUpdate = Updates.set("city", requestInsert.getAlphaString((int)(3 + (5 * Math.random()))));
			Bson zipCodeUpdate = Updates.set("zipCode", requestInsert.getZipCode());
			Bson streetNameUpdate = Updates.set("streetName", requestInsert.getAlphaString((int)(3 + (5 * Math.random()))));
			Bson streetNumberUpdate = Updates.set("streetNumber",(int)(Math.random()*100));
			Bson parentNamesUpdate = Updates.set("parentNames", requestInsert.getAlphaString((int)(3 + (5 * Math.random()))));
			Bson organisationUpdate = Updates.set("organisation",(int)(Math.random()*1000));
			List<Bson> listUpdates = new ArrayList<>();
			listUpdates.add(cityUpdate);
			listUpdates.add(zipCodeUpdate);
			listUpdates.add(streetNameUpdate);
			listUpdates.add(streetNumberUpdate);
			listUpdates.add(parentNamesUpdate);
			listUpdates.add(organisationUpdate);
			
			long startTime = System.currentTimeMillis();
			mongoConnection.citizens.updateOne(filter,listUpdates);
			long endTime = System.currentTimeMillis();
			requestExecutionTime = endTime-startTime;
		}
			
		
		Long[] requestStat = {requestType,requestExecutionTime};
		return requestStat;
		
	
	}
}
