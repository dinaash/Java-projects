import java.io.BufferedWriter;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;


import java.sql.SQLException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

public class mysqlConnection  {
	
	
	static FileWriter fw = null;
	static BufferedWriter bw = null;
	 // db parameters
    private static String urlW       = "jdbc:mysql://localhost:6446/biergercenter";
    private static String urlR       = "jdbc:mysql://localhost:6447/biergercenter";
    private static String user      = "root";
    private static String password  = "MyNewPass";
    
    public static List<Connection> connectionPoolW = new ArrayList<>();
    public static List<Connection> connectionPoolR = new ArrayList<>();
    public static List<Connection> usedConnections = new ArrayList<>();
    
    private static int INITIAL_POOL_SIZE = 149;
    
	
	public static void main(String[] args) {
	
		
	try {
    	 for (int i = 0; i < INITIAL_POOL_SIZE; i++) {
    		 connectionPoolW.add(DriverManager.getConnection(urlW, user, password));
    		 connectionPoolR.add(DriverManager.getConnection(urlR, user, password));
        }
			System.out.println("connection pools created");
			BlockingQueue<Long[]> q = new SynchronousQueue<Long[]>();
			MyThreadPoolExecutor threadPool = new MyThreadPoolExecutor(250);
			resultFileWriter c = new resultFileWriter(q,"C:\\Users\\Asus\\Documents\\thesis\\statisticsMysql3.txt");
			
			//for (int client = 0; client <30; client++) {
				
				/*threadPool.schedule(new insertRequest(q,(client)*10,(((client+1)*10)-1)),(client)*200, TimeUnit.MILLISECONDS);
				threadPool.schedule(new aggregateRequest (q,0),(client+((client-1)%2))*100, TimeUnit.MILLISECONDS);
				threadPool.schedule(new aggregateRequest (q,1),(client+((client-1)%2))*100, TimeUnit.MILLISECONDS);
				threadPool.schedule(new aggregateRequest (q,2),(client+((client-1)%2))*100, TimeUnit.MILLISECONDS);
				threadPool.schedule(new aggregateReadRequest (q,client),(client+((client-1)%2))*100, TimeUnit.MILLISECONDS);
				threadPool.schedule(new readAllRequest (q),(client+((client-1)%2))*100, TimeUnit.MILLISECONDS);
				threadPool.schedule(new readRequest (q,(client == 0) ? 0 : client-1),(client+((client-1)%2))*100, TimeUnit.MILLISECONDS);
				
				threadPool.schedule(new updateRequest (q,(client == 0) ? 0 : client-1),(client+((client-1)%2))*100, TimeUnit.MILLISECONDS);   
				*/
				
				//threadPool.execute(new insertRequest(q,(client)*10,(((client+1)*10)-1)));
				
				for (int client = 0; client <250; client ++) {
					threadPool.execute(new insertRequest(q,0,100));
					threadPool.execute(new aggregateRequest (q,0));
					threadPool.execute(new aggregateRequest (q,1));
					threadPool.execute(new aggregateRequest (q,2));
					threadPool.execute(new aggregateReadRequest (q));
					threadPool.execute(new readAllRequest (q));
					threadPool.execute(new readRequest (q));
					threadPool.execute(new updateRequest (q));  
				}
				 
				
				
				/*threadPool.scheduleAtFixedRate(new insertRequest(q,0,100), 0, 3, TimeUnit.SECONDS);
				threadPool.scheduleAtFixedRate(new aggregateRequest (q,0), 0, 3, TimeUnit.SECONDS);
				threadPool.scheduleAtFixedRate(new aggregateRequest (q,1), 0, 3, TimeUnit.SECONDS);
				threadPool.scheduleAtFixedRate(new aggregateRequest (q,2), 0, 3, TimeUnit.SECONDS);
				threadPool.scheduleAtFixedRate(new aggregateReadRequest (q), 0, 3, TimeUnit.MILLISECONDS);
				threadPool.scheduleAtFixedRate(new readAllRequest (q), 0, 3, TimeUnit.SECONDS);
				threadPool.scheduleAtFixedRate(new readRequest (q), 0, 3, TimeUnit.SECONDS);
				threadPool.scheduleAtFixedRate(new updateRequest (q), 0, 3, TimeUnit.SECONDS);*/
				
				/*new Thread(new insertRequest(q,(client)*10,(((client+1)*10)-1))).start();
				new Thread(new aggregateRequest (q,0)).start();
				new Thread(new aggregateRequest (q,1)).start();
				new Thread(new aggregateRequest (q,2)).start();
				new Thread(new aggregateReadRequest (q,client)).start();
				new Thread(new readAllRequest (q)).start();
				new Thread(new readRequest (q,(client == 0) ? 0 : client-1)).start();
				new Thread(new updateRequest (q,(client == 0) ? 0 : client-1)).start();  
				*/
			//}
			
		    new Thread(c).start();
		    
     	}
		catch (SQLException ex) {System.out.println(ex.getMessage());}
		
	}//end main
}// end class

