
import java.util.Arrays;
import org.bson.Document;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.io.BufferedWriter;
import java.io.FileWriter;

public class mongoConnection {
	
	
	static MongoCollection<Document> citizens = null;
	static FileWriter fw = null;
	static BufferedWriter bw = null;
	
	public static void main(String[] args) {
		MongoClient mongo =  null;
		 
		try
	        { //open connection, access the database, insert records, open and write records to file
			MongoClientOptions.Builder builder = MongoClientOptions.builder();
			builder.connectionsPerHost(40);
			MongoClientOptions sslOptions = builder.build();
	        mongo = new MongoClient(Arrays.asList(
	 				//new ServerAddress("localhost", 27017),
	 				//new ServerAddress("localhost", 27018),
	 				//new ServerAddress("localhost", 27019),
	 			
	 				//new ServerAddress("localhost", 27020),
	 				new ServerAddress("localhost", 27021),
	 				//new ServerAddress("localhost", 27022),
	 				//new ServerAddress("localhost", 27023),
	 				new ServerAddress("localhost", 27024),
	 				new ServerAddress("localhost", 27025)
	 				),sslOptions);System.out.println("connected");
	 		
			MongoDatabase database = mongo.getDatabase("biergerCenter");System.out.println("biergerCenter db accessed");
	 		
	 		citizens = database.getCollection("citizens");System.out.println("citizens collection accessed");
	 		
	 		
	 		
	 		BlockingQueue<Long[]> q = new SynchronousQueue<Long[]>();
	 		MyThreadPoolExecutor threadPool = new MyThreadPoolExecutor(250);
	 		resultFileWriter c = new resultFileWriter(q,"C:\\Users\\Asus\\Documents\\thesis\\statisticsMongo3.txt");
	 		
	 		for (int client = 0; client <250; client ++) {
				threadPool.execute(new requestInsert(q,0,100));
				threadPool.execute(new requestReadAll(q));
				threadPool.execute(new requestAggregate(q,0));
				threadPool.execute(new requestAggregate(q,1));
				threadPool.execute(new requestAggregate(q,2));
				threadPool.execute(new requestAggregateRead (q));
				threadPool.execute(new requestRead (q));
				threadPool.execute(new requestUpdate (q)); 
			}
	 		new Thread(c).start();
		    //requestDeleteAll rDA = new requestDeleteAll(q);
		    //new Thread(rDA).start();

	        }//end try
	        catch (Exception e) 
	        { 
	        	e.printStackTrace(); 
	        }
		
		
	
	}//end main	
	
}//end class
	

