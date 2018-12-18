package cassandra.writemodule;

import java.util.*;
import java.util.concurrent.TimeUnit;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.google.common.base.Stopwatch;

public class BulkLoader {

private final int threads;
private final String[] contactHosts;

public BulkLoader(int threads, String...contactHosts){
    this.threads = threads;
    this.contactHosts = contactHosts;
}

public void ingest(Iterator<Object[]> boundItemsIterator, String insertCQL) throws InterruptedException {
	
    Cluster cluster = Cluster.builder().addContactPoints(contactHosts).build();
    Session session = cluster.newSession();
    
    List<ResultSetFuture> futures = new ArrayList<>();
    
    final PreparedStatement statement = session.prepare(insertCQL);
    int count = 0;
    
    while (boundItemsIterator.hasNext()) {
    
    	BoundStatement boundStatement = statement.bind(boundItemsIterator.next());
        ResultSetFuture future = session.executeAsync(boundStatement);
        futures.add(future);
        count++;
        
        if(count % threads==0){
            futures.forEach(ResultSetFuture::getUninterruptibly);
            futures = new ArrayList<>();
        }
        
    }
    session.close();
    cluster.close();
}

public static void main(String[] args) throws InterruptedException {
    Iterator<Object[]> rows = new Iterator<Object[]>() {
        int i = 0;
        Random random = new Random();

        @Override
        public boolean hasNext() {
            return i!=100000;
        }

        @Override
        public Object[] next() {
            i++;
            return new Object[]{i, String.valueOf(random.nextLong())};
        }
    };

    System.out.println("Starting benchmark");
    
    Stopwatch watch = Stopwatch.createStarted();
    
    new BulkLoader(8, "127.0.0.1").ingest(rows, "INSERT INTO my_test.rows (id, value) VALUES (?,?)");
    System.out.println("total time seconds = " + watch.elapsed(TimeUnit.SECONDS));
    
    watch.stop();
	}
}