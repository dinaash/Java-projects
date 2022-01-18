import java.util.Arrays;
import java.util.concurrent.ScheduledThreadPoolExecutor;

class MyThreadPoolExecutor extends ScheduledThreadPoolExecutor{
	
	public MyThreadPoolExecutor(int corePoolSize) {
        super(corePoolSize);
    }
	
	 @Override
	    public void afterExecute(Runnable r, Throwable t) {
	        super.afterExecute(r, t);
	        
	        if (t != null) {
	            // Exception occurred
	            System.err.println("Uncaught exception is detected! " + t
	            + " st: " + Arrays.toString(t.getStackTrace()));
	            // ... Handle the exception
	            // Restart the runnable again
	            execute(r);
	            System.out.println("rerunning");
	        }
	 }

}
