package simpledb.systemtest;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;

import org.junit.Test;

import junit.framework.Assert;
import simpledb.*;

/**
 * Creates a heap file with 1024*500 tuples with two integer fields each.  Clears the buffer pool,
 * and performs a sequential scan through all of the pages.  If the growth in JVM usage
 * is greater than 2 MB due to the scan, the test fails.  Otherwise, the page eviction policy seems
 * to have worked.
 */
public class BufferPolicyTest {
    private static final long MEMORY_LIMIT_IN_MB = 2;
    private static final int BUFFER_PAGES = 10;
    public static final int DEFAULT_POLICY = 0;
    public static final int LRU_POLICY = 1;
    public static final int MRU_POLICY = 2;

    public static final int[] RANDOM_ACCESSES = new int[] {10,20,30,40,50,60,70,80,90,100,1,20,30,2,3,40,50,10,100,90,10,30,90,30,70,2,55,56,55,60};

    @Test public void testBufferReplacementPolicies() throws IOException, DbException, TransactionAbortedException {
	HeapPageId pid;
	Page page;

        System.out.println("BufferPolicyTest creating large table");
        HeapFile f = SystemTestUtil.createRandomHeapFile(2, 1024*50, null, null);
	System.out.println("BufferPolicyTest File Size is  ***"+ f.numPages() +"***  pages");

        System.out.println("-------------------------------------------------------------------");

        System.out.println("BufferPolicyTest testing LRU on scan");
        Database.resetBufferPool(BUFFER_PAGES);
	System.out.println("New Buffer, Size " + BUFFER_PAGES +" pages");
	Database.getBufferPool().setReplacePolicy(LRU_POLICY);

	System.out.println("Sequential scan of file ID: "+f.id()+", 3 times");
	SeqScan scan = new SeqScan(null, f.id(), "");
	for (int i=0;i<3;i++){
	    scan.open();
	    while (scan.hasNext()) {
		scan.next();
	    }
	    scan.close();
	}
        System.out.println("BufferPolicyTest scan complete, reporting number of hits and misses");

        System.out.println("Number of Hits: "+Database.getBufferPool().getNumHits());
        System.out.println("Number of Misses: "+Database.getBufferPool().getNumMisses());
       
        System.out.println("-------------------------------------------------------------------");
       

        System.out.println("BufferPolicyTest testing MRU on scan");
        Database.resetBufferPool(BUFFER_PAGES);
	System.out.println("New Buffer, Size " + BUFFER_PAGES +" pages");
	Database.getBufferPool().setReplacePolicy(MRU_POLICY);

	System.out.println("Sequential scan of file ID: "+f.id()+", 3 times");
        
	for (int i=0;i<3;i++){
	    scan.open();
	    while (scan.hasNext()) {
		scan.next();
	    }
	    scan.close();
	}

        System.out.println("BufferPolicyTest scan complete, reporting number of hits and misses");

        System.out.println("Number of Hits: "+Database.getBufferPool().getNumHits());
        System.out.println("Number of Misses: "+Database.getBufferPool().getNumMisses());

        System.out.println("-------------------------------------------------------------------");
       

        System.out.println("BufferPolicyTest testing LRU on scan");
        Database.resetBufferPool(50);
	System.out.println("New Buffer, Size " + 50 +" pages");
	Database.getBufferPool().setReplacePolicy(LRU_POLICY);

	System.out.println("Sequential scan of file ID: "+f.id()+", 3 times");
        
	for (int i=0;i<3;i++){
	    scan.open();
	    while (scan.hasNext()) {
		scan.next();
	    }
	    scan.close();
	}

        System.out.println("BufferPolicyTest scan complete, reporting number of hits and misses");

        System.out.println("Number of Hits: "+Database.getBufferPool().getNumHits());
        System.out.println("Number of Misses: "+Database.getBufferPool().getNumMisses());

        System.out.println("-------------------------------------------------------------------");
       

        System.out.println("BufferPolicyTest testing MRU on scan");
        Database.resetBufferPool(50);
	System.out.println("New Buffer, Size " + 50 +" pages");
	Database.getBufferPool().setReplacePolicy(MRU_POLICY);

	System.out.println("Sequential scan of file ID: "+f.id()+", 3 times");
        
	for (int i=0;i<3;i++){
	    scan.open();
	    while (scan.hasNext()) {
		scan.next();
	    }
	    scan.close();
	}

        System.out.println("BufferPolicyTest scan complete, reporting number of hits and misses");

        System.out.println("Number of Hits: "+Database.getBufferPool().getNumHits());
        System.out.println("Number of Misses: "+Database.getBufferPool().getNumMisses());
        System.out.println("-------------------------------------------------------------------");
       

        System.out.println("BufferPolicyTest testing LRU on scan");
        Database.resetBufferPool(120);
	System.out.println("New Buffer, Size " + 120 +" pages");
	Database.getBufferPool().setReplacePolicy(LRU_POLICY);

	System.out.println("Sequential scan of file ID: "+f.id()+", 3 times");
        
	for (int i=0;i<3;i++){
	    scan.open();
	    while (scan.hasNext()) {
		scan.next();
	    }
	    scan.close();
	}

        System.out.println("BufferPolicyTest scan complete, reporting number of hits and misses");

        System.out.println("Number of Hits: "+Database.getBufferPool().getNumHits());
        System.out.println("Number of Misses: "+Database.getBufferPool().getNumMisses());

        System.out.println("-------------------------------------------------------------------");
       

        System.out.println("BufferPolicyTest testing MRU on scan");
        Database.resetBufferPool(120);
	System.out.println("New Buffer, Size " + 120 +" pages");
	Database.getBufferPool().setReplacePolicy(MRU_POLICY);

	System.out.println("Sequential scan of file ID: "+f.id()+", 3 times");
        
	for (int i=0;i<3;i++){
	    scan.open();
	    while (scan.hasNext()) {
		scan.next();
	    }
	    scan.close();
	}

        System.out.println("BufferPolicyTest scan complete, reporting number of hits and misses");

        System.out.println("Number of Hits: "+Database.getBufferPool().getNumHits());
        System.out.println("Number of Misses: "+Database.getBufferPool().getNumMisses());

        System.out.println("-------------------------------------------------------------------");
       

        System.out.println("BufferPolicyTest testing LRU on random accesses (checks that last usage is recorded in buffer)");
        Database.resetBufferPool(BUFFER_PAGES);
	System.out.println("New Buffer, Size " + BUFFER_PAGES +" pages");
	Database.getBufferPool().setReplacePolicy(LRU_POLICY);
	
	for(int i=0; i<RANDOM_ACCESSES.length; i++){
	    pid = new HeapPageId(f.id(), RANDOM_ACCESSES[i]);
	    page = Database.getBufferPool().getPage(null, pid, Permissions.READ_ONLY);
	}
	
        System.out.println("BufferPolicyTest random accesses complete, reporting number of hits and misses");

        System.out.println("Number of Hits: "+Database.getBufferPool().getNumHits());
        System.out.println("Number of Misses: "+Database.getBufferPool().getNumMisses());

        System.out.println("-------------------------------------------------------------------");
       

        System.out.println("BufferPolicyTest testing MRU on scan");
        Database.resetBufferPool(BUFFER_PAGES);
	System.out.println("New Buffer, Size " + BUFFER_PAGES +" pages");
	Database.getBufferPool().setReplacePolicy(MRU_POLICY);

        System.out.println("BufferPolicyTest random accesses complete, reporting number of hits and misses");
        
	for(int i=0; i<RANDOM_ACCESSES.length; i++){
	    pid = new HeapPageId(f.id(), RANDOM_ACCESSES[i]);
	    page = Database.getBufferPool().getPage(null, pid, Permissions.READ_ONLY);
	}
	

        System.out.println("BufferPolicyTest scan complete, reporting number of hits and misses");

        System.out.println("Number of Hits: "+Database.getBufferPool().getNumHits());
        System.out.println("Number of Misses: "+Database.getBufferPool().getNumMisses());


    }
    

    /** Make test compatible with older version of ant. */
    public static junit.framework.Test suite() {
        return new junit.framework.JUnit4TestAdapter(BufferPolicyTest.class);
    }
}
