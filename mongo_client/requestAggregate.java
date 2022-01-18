
import static com.mongodb.client.model.Accumulators.*;
import static com.mongodb.client.model.Aggregates.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.client.AggregateIterable;

import static com.mongodb.client.model.Projections.*;
import static com.mongodb.client.model.Sorts.*;

public class requestAggregate implements Runnable{
	
	private final BlockingQueue<Long[]> queue;
	private int choice;
	
	requestAggregate(BlockingQueue<Long[]> q, int c) { queue = q;choice=c;}
	
	
	public void run() {
		
		try {
			queue.put(produceRequestAggregate(choice));	
		}
		catch (InterruptedException ex) {
			Thread.currentThread().interrupt();  // set interrupt flag
			ex.printStackTrace();
        }
	}// end run
	
	
	public Long[] produceRequestAggregate(int i) {
		
		long requestType = 8;
		
		long requestExecutionTime = 0;
		
		int loop_counter = 0;
		
		if (i == 0) //find max
			{
			while (requestExecutionTime == 0) {
				loop_counter ++;
				Bson group = group("$streetNumber", sum("pop",1));
				Bson project = project(fields(excludeId(), include("streetNumber","pop")));
				Bson sort = sort(descending("pop"));
				Bson limit = limit(1);
				long startTime = System.currentTimeMillis();
				mongoConnection.citizens.aggregate(Arrays.asList(group, project, sort, limit));
				long endTime = System.currentTimeMillis();
				
				requestExecutionTime = (long)(endTime-startTime);
			}
			System.out.println(i+"th aggregate loop ran "+loop_counter+" times");
			Long[] requestStat = {requestType,requestExecutionTime};
			return requestStat;
			}
		
		else if (i == 1) //find min
			{
			
			while (requestExecutionTime == 0) {
				loop_counter ++;
				Bson group = group("$streetNumber", sum("pop", 1));
				Bson project = project(fields(excludeId(), include("streetNumber","pop")));
				Bson sort = sort(ascending("pop"));
				Bson limit = limit(1);long startTime = System.currentTimeMillis();
				mongoConnection.citizens.aggregate(Arrays.asList(group, project, sort, limit));       
				long endTime = System.currentTimeMillis();
				requestExecutionTime = (long)(endTime-startTime);
				
			}
			System.out.println(i+"th aggregate loop ran "+loop_counter+" times");
			Long[] requestStat = {requestType,requestExecutionTime};
			return requestStat;
			}
		
		else // find average
			{
			
			while (requestExecutionTime == 0) {
				loop_counter ++;
				Bson group = group("$streetNumber", avg("avgDateBirth", "$dateBirth"));
				Bson project = project(fields(excludeId(), include("streetNumber","avgDateBirth")));
				long startTime = System.currentTimeMillis();
				mongoConnection.citizens.aggregate(Arrays.asList(group,project));
				long endTime = System.currentTimeMillis();
				requestExecutionTime = (long)(endTime-startTime);
			}
			System.out.println(i+"th aggregate loop ran "+loop_counter+" times");
			Long[] requestStat = {requestType,requestExecutionTime};
			
			return requestStat;
			}	
		
	}//end method

}

	 
/*
db.citizens.aggregate([{ $group: { _id: "$streetNumber", pop: { $sum: 1 } } },{ $project: { "streetNumber": 1, "pop": 1} },{ $sort: { pop : -1 } },{ $limit: 1 }])

db.citizens.explain("executionStats").aggregate([{ $group: { _id: "$gender", pop: { $sum: 1 } } }])





               
                        
                        
                        
*/                      
