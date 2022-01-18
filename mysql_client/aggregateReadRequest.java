
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.BlockingQueue;


public class aggregateReadRequest implements Runnable{
	
	private final BlockingQueue<Long[]> queue;
	
	
	aggregateReadRequest(BlockingQueue<Long[]> q) { queue = q;}
	
	public void run() {
		
		try {
			queue.put(produceaggregateReadRequest());	
		}
		catch (InterruptedException ex) {
			Thread.currentThread().interrupt();  // set interrupt flag
			ex.printStackTrace();
        }
		
	}// end run
	
	public Long[] produceaggregateReadRequest () {
		long requestType = 3;
		long requestExecutionTime = 0;
		
		while (requestExecutionTime == 0) {
			try {	
			
				Connection connection = mysqlConnection.connectionPoolR
				          .remove(mysqlConnection.connectionPoolR.size() - 1);
				mysqlConnection.usedConnections.add(connection);
				System.out.println(connection.isClosed());
				
				PreparedStatement statement;
				
				int random_citizen = (int)(Math.random()*100000);
				String query = "SELECT ID, dateBirth, streetNumber "
						+ "FROM citizens "
						+ "WHERE streetNumber = (SELECT streetNumber FROM citizens WHERE ID ="+random_citizen+")"
						+ "AND dateBirth >= 1000+(SELECT dateBirth FROM citizens WHERE ID="+random_citizen+")";
				
				long startTime = System.currentTimeMillis();
				statement = connection.prepareStatement(query);
				boolean rs = statement.execute();
				long endTime = System.currentTimeMillis();
				statement.close();
				requestExecutionTime = (long)(endTime-startTime);
				
				mysqlConnection.connectionPoolR.add(connection);
				mysqlConnection.usedConnections.remove(connection);
				System.out.println("aggregateRead result: "+rs);
			
			
			} 
			catch (SQLException e) {
				e.printStackTrace();
				requestExecutionTime = 0;
			}//end try catch
		}// end while
		Long[] requestStat = {requestType,requestExecutionTime};
		return requestStat;
		
	}//end method produceReadAllRequest

}//end class
