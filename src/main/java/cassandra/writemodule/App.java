package cassandra.writemodule;

import java.time.Duration;
import java.time.Instant;
import java.util.Calendar;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.PropertyConfigurator;

import com.datastax.driver.core.Session;

public class App 
{
    public static void main( String[] args ) {

    	BasicConfigurator.configure();
    	//String log4jproperties = "src/main/java/log4j.properties";
    	String log4jproperties = "/scannet/conf/cassandra.properties";
    	PropertyConfigurator.configure(log4jproperties);	
    	Logger log = Logger.getLogger(log4jproperties);
    	
    	
    	// code goes here
    	
    	double start = 90;
    	double end = 5;
    	long b = 10000000; 
    	
    	Instant inicio = Instant.now();
    	Connector con = new Connector();

    		try {
   			
    			con.connect("kpscannet", 9042);
    			Session bulkInsert = con.getSession();
    			
    			long startTime = System.currentTimeMillis();
    			
				for (int i = 0; i < b ; i++) {    
															 
					if (i % 100000 == 0) {						
						long finishTime = System.currentTimeMillis();
				    	long elapsedTime = finishTime - startTime;
				    	
				    	System.out.println(TimeUnit.MILLISECONDS.toMinutes(elapsedTime));
				    
					}											
    				   					
					long timeNow = 0;				    				    		
    				double random = new Random().nextDouble();
    				double lat = start + (random * (end - start));
    				    				
    				//String nombre = "PRBEL" + i ;
    				// Metric met = new Metric(timeNow+i, lat, "PRBEL99937", "BBIP");    	    				
    				Metric met = new Metric(timeNow+i, lat, "PRBEL413607", "BBIP");
    				bulkInsert.execute(met.toString());
    				//System.out.println(met.toString());
    			
    			}
    			
    			con.close();

    		} catch (Exception e) {
    			System.out.println("No se puede conectar \n" + e);
    		}	
    		//long fin = System.currentTimeMillis();
    		Instant fin = Instant.now();
    		//System.out.println(fin - inicio);		
    		System.out.println("Elapsed : " + Duration.between(inicio, fin));
    		    		
    }
}
