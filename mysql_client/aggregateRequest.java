
import java.util.concurrent.BlockingQueue;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;



public class aggregateRequest implements Runnable {
	
	private final BlockingQueue<Long[]> queue;
	private int choice;
	Boolean result = false;
	
	aggregateRequest(BlockingQueue<Long[]> q, int c) { queue = q;choice=c;}
	
	public void run() {
		
		try {
			queue.put(produceaggregateRequest(choice));	
		}
		catch (InterruptedException ex) {Thread.currentThread().interrupt();  // set interrupt flag
			ex.printStackTrace();
        }
	}// end run
	
	
	public Long[] produceaggregateRequest(int i) {
		
		long requestType = 8;
		long requestExecutionTime = 0;
		
		
		while (requestExecutionTime == 0) {	
		
		try {
			String query = null;
			if (i == 0) //find max
			{
			query = " select streetNumber, COUNT(*) as pop FROM citizens GROUP BY streetNumber ORDER BY pop DESC LIMIT 1 ";
			}
		
			else if (i == 1) //find min
			{
			query = " select streetNumber, COUNT(*) as pop FROM citizens GROUP BY streetNumber ORDER BY pop LIMIT 1 ";
			
			}
		
			else // find average
			{
			query = " select streetNumber, FROM_UNIXTIME(AVG(UNIX_TIMESTAMP(dateBirth))) as avgDateBirth FROM citizens GROUP BY streetNumber ";
			} 
			
			
				Connection connection = mysqlConnection.connectionPoolR
				          .remove(mysqlConnection.connectionPoolR.size() - 1);
				
				mysqlConnection.usedConnections.add(connection);
				PreparedStatement statement;
				
				
				long startTime = System.currentTimeMillis();
				statement = connection.prepareStatement(query);
				statement.execute();
				long endTime = System.currentTimeMillis();
				statement.close();
				requestExecutionTime = (long)(endTime-startTime);
				
				mysqlConnection.connectionPoolR.add(connection);
				mysqlConnection.usedConnections.remove(connection);
			
					
		} catch (SQLException e) {
			e.printStackTrace();
			requestExecutionTime = 0;
		}//end try catch
		}//end while
		
		Long[] requestStatSuccess = {requestType,requestExecutionTime};
		return requestStatSuccess;
		
	}//end method

}
