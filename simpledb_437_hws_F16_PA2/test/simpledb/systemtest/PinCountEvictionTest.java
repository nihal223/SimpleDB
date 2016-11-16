package simpledb.systemtest;

import java.io.IOException;

import junit.framework.Assert;

import org.junit.Test;

import simpledb.BufferPool;
import simpledb.Database;
import simpledb.DbException;
import simpledb.HeapFile;
import simpledb.HeapPageId;
import simpledb.SeqScan;
import simpledb.TransactionAbortedException;

/**
 * Creates a heap file with 1024*500 tuples with two integer fields each.  Clears the buffer pool,
 * and performs a sequential scan through all of the pages.  If the growth in JVM usage
 * is greater than 2 MB due to the scan, the test fails.  Otherwise, the page eviction policy seems
 * to have worked.
 */
public class PinCountEvictionTest {
    private static final long MEMORY_LIMIT_IN_MB = 2;
    private static final int BUFFER_PAGES = 16;
    public static final int LRU_POLICY = 1;

    @Test public void testHeapFileScanWithManyPages() throws IOException, DbException, TransactionAbortedException {
        System.out.println("EvictionTest creating large table");
        HeapFile f = SystemTestUtil.createRandomHeapFile(2, 1024*700, null, null);
        System.out.println("EvictionTest scanning large table: "+f.numPages()+" pages");
        Database.resetBufferPool(BUFFER_PAGES);
        Database.getBufferPool().setReplacePolicy(LRU_POLICY);
        long beginMem = SystemTestUtil.getMemoryFootprint();
        System.out.println("Memory Limit: "+beginMem);
        System.out.println("seqscan fileid: "+f.id());
        SeqScan scan = new SeqScan(null, f.id(), "");
        BufferPool buffer = Database.getBufferPool();
        int num_page = 0;
        int prev_page = 0;
        int index = -1;
        HeapPageId pid;
        scan.open();
        while (scan.hasNext()) {
	    	num_page = scan.getPagesRead();
	    	pid = new HeapPageId(f.id(), num_page-1);
	    	index = buffer.getBufferIndex(pid);
	    	if ((index != -1) && (num_page != prev_page)) {
		    	try {
					buffer.unpinPage(index, null, false);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		    	prev_page = num_page;
	    	}
            scan.next();
        }
        System.out.println("EvictionTest scan complete, testing memory usage of scan");
        long endMem = SystemTestUtil.getMemoryFootprint();
        long memDiff = (endMem - beginMem) / (1<<20);
        System.out.println("Memory Footprint: "+endMem);
       
        if (memDiff > MEMORY_LIMIT_IN_MB) {
            Assert.fail("Did not evict enough pages.  Scan took " + memDiff + " MB of RAM, when limit was " + MEMORY_LIMIT_IN_MB);
        }
    }
    
    /** Make test compatible with older version of ant. */
    public static junit.framework.Test suite() {
        return new junit.framework.JUnit4TestAdapter(PinCountEvictionTest.class);
    }
}
