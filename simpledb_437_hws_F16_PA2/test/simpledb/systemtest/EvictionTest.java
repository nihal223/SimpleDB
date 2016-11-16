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
public class EvictionTest {
    private static final long MEMORY_LIMIT_IN_MB = 2;
    private static final int BUFFER_PAGES = 16;

    @Test public void testHeapFileScanWithManyPages() throws IOException, DbException, TransactionAbortedException {
        System.out.println("EvictionTest creating large table");
        HeapFile f = SystemTestUtil.createRandomHeapFile(2, 1024*700, null, null);
        System.out.println("EvictionTest scanning large table: "+f.numPages()+" pages");
        Database.resetBufferPool(BUFFER_PAGES);
        long beginMem = SystemTestUtil.getMemoryFootprint();
        System.out.println("Memory Limit: "+beginMem);
	System.out.println("seqscan fileid: "+f.id());
	SeqScan scan = new SeqScan(null, f.id(), "");
        scan.open();
        while (scan.hasNext()) {
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
        return new junit.framework.JUnit4TestAdapter(EvictionTest.class);
    }
}
