


import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;



public class requestRead implements Runnable{
	
	private final BlockingQueue<Long[]> queue;
	
	
	requestRead(BlockingQueue<Long[]> q) { queue = q;}
	
	public void run() {
		
		try {
			queue.put(produceRequestRead());	
		}
		catch (InterruptedException ex) {
			Thread.currentThread().interrupt();  // set interrupt flag
			ex.printStackTrace();
        }
	}// end run
	
	public Long[] produceRequestRead () {
		
		long requestType = 4;
		long requestExecutionTime = 0;
		
		while (requestExecutionTime == 0) {
			Bson filters = Filters.and(
					Filters.gt("ID", (int)(Math.random()*100000)),
					Filters.gte("dateBirth", (requestInsert.randomDateOfBirth())),
					Filters.lte("streetNumber", 50));
			
			long startTime = System.currentTimeMillis();
			mongoConnection.citizens.find(filters);
			long endTime = System.currentTimeMillis();
			requestExecutionTime = endTime-startTime;
			
			
		}
		Long[] requestStat = {requestType,requestExecutionTime};
		return requestStat;
	}//end method produceReadAllRequest

}//end class







