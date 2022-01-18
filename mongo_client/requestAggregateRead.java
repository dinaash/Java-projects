

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import org.bson.Document;
import org.bson.conversions.Bson;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;


public class requestAggregateRead implements Runnable{
	
	private final BlockingQueue<Long[]> queue;
	
	requestAggregateRead(BlockingQueue<Long[]> q) { queue = q;}
	
	public void run() {
		
		try {
			queue.put(produceRequestAggregateRead());	
		}
		catch (InterruptedException ex) {
			Thread.currentThread().interrupt();  // set interrupt flag
			ex.printStackTrace();
        }
	}// end run
	
	public Long[] produceRequestAggregateRead () {
		long requestType = 3;
		long requestExecutionTime = 0;
		
		while (requestExecutionTime == 0) {
			Bson match = new Document ("$match",new Document("ID",(int)(Math.random()*100000)));
			
			
			Bson innerMatch = new Document ("$match",
					new Document("$expr",
							new Document("$and",
							  Arrays.asList(
								new Document("$gte", Arrays.asList("$dateBirth",new Document ("$add", Arrays.asList("$$bday",1000)))),
								new Document ("$eq", Arrays.asList("$streetNumber", "$$street_number"))				  
							  )				
							)
					)
			);
			Bson innerProject = new Document ("$project",new Document("streetNumber",1).append("dateBirth",1));
			List<Bson> pipeline = new ArrayList<>();
			pipeline.add(innerMatch);
			pipeline.add(innerProject);
			Bson lookup = new Document ("$lookup",new Document("from","citizens")
					.append("let", new Document ("street_number","$streetNumber").append("bday","$dateBirth"))
					.append("pipeline",pipeline)
					.append("as","neighbours"));
			
			List<Bson> filters = new ArrayList<>();
			filters.add(match);
			filters.add(lookup);
			long startTime = System.currentTimeMillis();		
			mongoConnection.citizens.aggregate(filters);
			long endTime = System.currentTimeMillis();
			requestExecutionTime = endTime-startTime;
			
		}
		Long[] requestStat = {requestType,requestExecutionTime};
		return requestStat;
	}//end method produceReadAllRequest

}//end class



                    
                    
