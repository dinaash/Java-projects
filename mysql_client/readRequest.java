
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.BlockingQueue;



public class readRequest implements Runnable{
	
	private final BlockingQueue<Long[]> queue;
	
	readRequest(BlockingQueue<Long[]> q) { queue = q;}
	
	public void run() {
		
		try {
			queue.put(producereadRequest());	
		}
		catch (InterruptedException ex) {
			Thread.currentThread().interrupt();  // set interrupt flag
			ex.printStackTrace();
        }
		
	}// end run
	
	public Long[] producereadRequest () {
		long requestType = 4;
		long requestExecutionTime = 0;
		
		while (requestExecutionTime == 0) {
			try {
				
				
					Connection connection = mysqlConnection.connectionPoolR
					          .remove(mysqlConnection.connectionPoolR.size() - 1);
					
					mysqlConnection.usedConnections.add(connection);
					PreparedStatement statement;
					
					String query = "select ID, dateBirth, streetNumber "
							+ "FROM citizens WHERE ID >"+(int)(Math.random()*100000)+" AND dateBirth >=\""+(insertRequest.randomDateOfBirth())+"\" AND streetNumber <="+50;
					
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
		
		Long[] requestStat = {requestType,requestExecutionTime};
		return requestStat;
		
	}//end method produceReadAllRequest

}//end class



