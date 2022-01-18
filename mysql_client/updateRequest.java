
import java.sql.Connection;
import java.sql.PreparedStatement;

import java.sql.SQLException;

import java.util.concurrent.BlockingQueue;




public class updateRequest implements Runnable {

	private final BlockingQueue<Long[]> queue;
	
	
	updateRequest(BlockingQueue<Long[]> q) {  queue = q;}
	
	public void run() {
		
		try {
			queue.put(produceUpdateRequest ());
			
		}
		catch (InterruptedException ex) {
			Thread.currentThread().interrupt();  // set interrupt flag
			ex.printStackTrace();
        }
		
	}// end run
	
	public Long[] produceUpdateRequest () {
		
		long requestType = 5;
		long requestExecutionTime = 0;
		
		while (requestExecutionTime == 0) {
			try {
				
					Connection connection = mysqlConnection.connectionPoolW
					          .remove(mysqlConnection.connectionPoolW.size() - 1);
					
					mysqlConnection.usedConnections.add(connection);
					
					
					PreparedStatement statement;
					
					
					String query = " UPDATE citizens "
							+ "SET "
							+ "city=\""+insertRequest.getAlphaString((int)(3 + (5 * Math.random())))+"\""
							+ ", zipCode=\""+insertRequest.getZipCode()+"\""
							+ ", streetName=\""+insertRequest.getAlphaString((int)(3 + (5 * Math.random())))+"\""
							+ ", streetNumber="+(int)(Math.random()*100)
							+ ", parentNames=\""+insertRequest.getAlphaString((int)(3 + (5 * Math.random())))+"\""
							+ ", organisation="+(int)(Math.random()*1000)
							+" WHERE ID = "+(int)(Math.random()*100000);
					
					long startTime = System.currentTimeMillis();
					statement = connection.prepareStatement(query);
					statement.executeUpdate(query);
					long endTime = System.currentTimeMillis();
					statement.close();
					requestExecutionTime = (long)(endTime-startTime);
					mysqlConnection.connectionPoolW.add(connection);
					mysqlConnection.usedConnections.remove(connection);
				
				} catch (SQLException e) {
					e.printStackTrace();
					requestExecutionTime = 0;
				} catch (ArrayIndexOutOfBoundsException e) {
					e.printStackTrace();
					requestExecutionTime = 0;
				}
				
				//end try catch
		}//end while
		Long[] requestStat = {requestType,requestExecutionTime};
		return requestStat;
		
	}//end method
}
