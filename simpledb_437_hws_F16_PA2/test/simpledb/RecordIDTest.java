package simpledb;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import junit.framework.JUnit4TestAdapter;

public class RecordIDTest {

    private static RecordID hrid;

    @Before public void setUp() {
        HeapPageId hpid = new HeapPageId(-1, 2);
        hrid = new RecordID(hpid, 3);

    }

    /**
     * Unit test for RecordID.pageid()
     */
    @Test public void pageid() {
        HeapPageId hpid = new HeapPageId(-1, 2);
        assertEquals(hpid, hrid.pageid());

    }

    /**
     * Unit test for RecordID.tupleno()
     */
    @Test public void tupleno() {
        assertEquals(3, hrid.tupleno());
    }

    /**
     * JUnit suite target
     */
    public static junit.framework.Test suite() {
        return new JUnit4TestAdapter(RecordIDTest.class);
    }
}

