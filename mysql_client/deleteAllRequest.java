
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.BlockingQueue;



public class deleteAllRequest implements Runnable{
	
	private final BlockingQueue<Long[]> queue;
	
	deleteAllRequest(BlockingQueue<Long[]> q) { queue = q;}
	
	public void run() {
		
		try {
			queue.put(producedeleteAllRequest());
			
		}
		catch (InterruptedException ex) {
			Thread.currentThread().interrupt();  // set interrupt flag
			ex.printStackTrace();
        }
	}// end run
	
	public Long[] producedeleteAllRequest () {
		
		long requestType = 7;
		long requestExecutionTime = 0;
		
		while (requestExecutionTime == 0) {
			try {
				
					Connection connection = mysqlConnection.connectionPoolW
					          .remove(mysqlConnection.connectionPoolW.size() - 1);
					mysqlConnection.usedConnections.add(connection);
					PreparedStatement statement;
					
					String query = "DELETE FROM citizens WHERE ID >=0 ";
					
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
			}
		}//end while
		Long[] requestStat = {requestType,requestExecutionTime};
		return requestStat;
	}//end method

}

