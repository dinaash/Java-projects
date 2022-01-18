import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import java.util.concurrent.BlockingQueue;


public class readAllRequest implements Runnable{
	
	private final BlockingQueue<Long[]> queue;
	
	readAllRequest(BlockingQueue<Long[]> q) { queue = q;}
	
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
			try {
				
					Connection connection = mysqlConnection.connectionPoolR
					          .remove(mysqlConnection.connectionPoolR.size() - 1);
					
					mysqlConnection.usedConnections.add(connection);
					PreparedStatement statement;
					
					String query = "select * FROM citizens ";
					
					long startTime = System.currentTimeMillis();
					statement = connection.prepareStatement(query);
					statement.execute();
					long endTime = System.currentTimeMillis();
					statement.close();
					
					requestExecutionTime = (long)(endTime-startTime);
					mysqlConnection.connectionPoolR.add(connection);
					mysqlConnection.usedConnections.remove(connection);
				}
				catch (SQLException e) {
					e.printStackTrace();
					requestExecutionTime = 0;
				}//end try catch
		}//end while
		
				
		Long[] requestStat = {requestType,requestExecutionTime};
		return requestStat;
	}//end method produceReadAllRequest

}//end class