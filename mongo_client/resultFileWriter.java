import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;

public class resultFileWriter implements Runnable {

	private BlockingQueue<Long[]> queue;
	private String fileUrl = null;
   
	resultFileWriter(BlockingQueue<Long[]> q, String url) { queue = q; fileUrl = url;}
   
	
	public void run() {
     try {
       while (true) { consume(queue.take()); System.out.println("printing to file");}
     } //end try
     catch (InterruptedException ex) { 
    	 ex.printStackTrace();
    	 }//end catch
   }//end run
   
	
	void consume (Long[] requestStat) {
		
	    try {
	    	mongoConnection.fw = new FileWriter(fileUrl, true);
	    	mongoConnection.bw = new BufferedWriter(mongoConnection.fw);
			mongoConnection.bw.write(requestStat[0]+","+requestStat[1]);
			mongoConnection.bw.newLine();
			if(mongoConnection.bw != null) mongoConnection.bw.close();
   	     	if(mongoConnection.fw != null) mongoConnection.fw.close();
		    System.out.println("all connections closed"); 
			
		} //end try
	    catch (IOException e) {
			e.printStackTrace();
		}//end catch
	    
	}
}
