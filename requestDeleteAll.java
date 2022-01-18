import java.util.concurrent.BlockingQueue;

import org.bson.Document;

import com.mongodb.client.model.Filters;

public class requestDeleteAll implements Runnable{
	
	private final BlockingQueue<Long[]> queue;
	
	requestDeleteAll(BlockingQueue<Long[]> q) { queue = q;}
	
	public void run() {
		
		try {
			queue.put(produceRequestDeleteAll());
			
		}
		catch (InterruptedException ex) {
			Thread.currentThread().interrupt();  // set interrupt flag
			ex.printStackTrace();
        }
	}// end run
	
	public Long[] produceRequestDeleteAll () {
		
		long requestType = 7;
		long requestExecutionTime = 0;
		
		while (requestExecutionTime == 0) {
			long startTime = System.currentTimeMillis();
			mongoConnection.citizens.deleteMany(new Document());
			long endTime = System.currentTimeMillis();
			requestExecutionTime = endTime-startTime;
		}
			
		Long[] requestStat = {requestType,requestExecutionTime};
		return requestStat;
	
	}

}
