import java.util.concurrent.BlockingQueue;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;

public class requestDelete implements Runnable{
	
	private final BlockingQueue<Long[]> queue;
	private final int key;
	
	requestDelete(BlockingQueue<Long[]> q, int searchKey) { queue = q; key=searchKey;}
	
	public void run() {
		
		try {
			queue.put(produceRequestDelete (key));
			
		}
		catch (InterruptedException ex) {
			Thread.currentThread().interrupt();  // set interrupt flag
			ex.printStackTrace();
        }
	}// end run
	
	public Long[] produceRequestDelete (int key) {
		
		long requestType = 6;
		long requestExecutionTime = 0;
		
		while (requestExecutionTime == 0) {
			long startTime = System.currentTimeMillis();
			mongoConnection.citizens.deleteOne(Filters.eq("ID", key));
			long endTime = System.currentTimeMillis();
			requestExecutionTime = endTime-startTime;
		}
			
		Long[] requestStat = {requestType,requestExecutionTime};
		return requestStat;
	
	}

}
