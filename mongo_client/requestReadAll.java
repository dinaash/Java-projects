import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.bson.Document;
import org.bson.types.ObjectId;

import com.mongodb.client.FindIterable;
import com.mongodb.client.model.InsertManyOptions;




public class requestReadAll implements Runnable{
	
	private final BlockingQueue<Long[]> queue;
	
	
	requestReadAll(BlockingQueue<Long[]> q) { queue = q;}
	
	public void run() {
		
		try {
			queue.put(produceRequestReadAll());	
		}
		catch (InterruptedException ex) {
			Thread.currentThread().interrupt();  // set interrupt flag
			ex.printStackTrace();
        }
	}// end run
	
	public Long[] produceRequestReadAll () {
		long requestType = 2;
		long requestExecutionTime = 0;
		
		while (requestExecutionTime == 0) {
			long startTime = System.currentTimeMillis();
			mongoConnection.citizens.find();
			long endTime = System.currentTimeMillis();
			requestExecutionTime = endTime-startTime;
		}
		Long[] requestStat = {requestType,requestExecutionTime};
		return requestStat;
	}//end method produceReadAllRequest

}//end class
