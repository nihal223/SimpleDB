package simpledb;

import simpledb.systemtest.SystemTestUtil;

import java.util.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import junit.framework.JUnit4TestAdapter;

public class HeapFileReadTest {
    private HeapFile hf;
    private TransactionId tid;

    /**
     * Set up initial resources for each unit test.
     */
    @Before
    public void setUp() throws Exception {
        hf = SystemTestUtil.createRandomHeapFile(2, 20, null, null);
        tid = new TransactionId();
    }

    @After
    public void tearDown() throws Exception {
        Database.getBufferPool().transactionComplete(tid);
    }

    /**
     * Unit test for HeapFile.id()
     */
    @Test
    public void id() throws Exception {
        int id = hf.id();

        // NOTE(ghuo): the value could be anything. test determinism, at least.
        assertEquals(id, hf.id());
        assertEquals(id, hf.id());

        HeapFile other = SystemTestUtil.createRandomHeapFile(1, 1, null, null);
        assertTrue(id != other.id());
    }

    /**
     * Unit test for HeapFile.numPages()
     */
    @Test
    public void numPages() throws Exception {
        assertEquals(1, hf.numPages());
        // assertEquals(1, empty.numPages());
    }

    /**
     * Unit test for HeapFile.readPage()
     */
    @Test
    public void readPage() throws Exception {
        HeapPageId pid = new HeapPageId(hf.id(), 0);
        HeapPage page = (HeapPage) hf.readPage(pid);

        // NOTE(ghuo): we try not to dig too deeply into the Page API here; we
        // rely on HeapPageTest for that. perform some basic checks.
	// assertEquals(492, page.getNumEmptySlots());
	//Amelie: changed the way the number of slots (header are part of 4096 now) are kept for better joins, so the number of empty slots is decreased by 8
	assertEquals(484, page.getNumEmptySlots());
        assertTrue(page.getSlot(1));
        assertFalse(page.getSlot(20));
    }

    @Test
    public void testIteratorBasic() throws Exception {
        HeapFile smallFile = SystemTestUtil.createRandomHeapFile(2, 3, null,
                null);

        DbFileIterator it = smallFile.iterator(tid);
        // Not open yet
        assertFalse(it.hasNext());
        try {
            it.next();
            fail("expected exception");
        } catch (NoSuchElementException e) {
        }

        it.open();
        int count = 0;
        while (it.hasNext()) {
            assertNotNull(it.next());
            count += 1;
        }
        assertEquals(3, count);
        it.close();
    }

    @Test
    public void testIteratorClose() throws Exception {
        // make more than 1 page. Previous closed iterator would start fetching
        // from page 1.
        HeapFile twoPageFile = SystemTestUtil.createRandomHeapFile(2, 520,
                null, null);

        DbFileIterator it = twoPageFile.iterator(tid);
        it.open();
        assertTrue(it.hasNext());
        it.close();
        try {
            it.next();
            fail("expected exception");
        } catch (NoSuchElementException e) {
        }
        // close twice is harmless
        it.close();
    }

    /**
     * JUnit suite target
     */
    public static junit.framework.Test suite() {
        return new JUnit4TestAdapter(HeapFileReadTest.class);
    }
}
