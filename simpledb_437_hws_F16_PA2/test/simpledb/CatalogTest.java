package simpledb;

import java.util.NoSuchElementException;

import simpledb.TestUtil.SkeletonFile;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import junit.framework.Assert;
import junit.framework.JUnit4TestAdapter;

public class CatalogTest {
    private static String name = "test";
    
    @Before public void setUp() throws Exception {
        Database.getCatalog().clear();
        Database.getCatalog().addTable(new SkeletonFile(-1), Utility.getTupleDesc(2));
        Database.getCatalog().addTable(new SkeletonFile(-2), Utility.getTupleDesc(2), name);
    }

    /**
     * Unit test for Catalog.getTupleDesc()
     */
    @Test public void getTupleDesc() throws Exception {
        TupleDesc expected = Utility.getTupleDesc(2);
        TupleDesc actual = Database.getCatalog().getTupleDesc(-1);

        assertEquals(expected, actual);
    }

    /**
     * Unit test for Catalog.getTableId()
     */
    @Test public void getTableId() {
        assertEquals(Database.getCatalog().getTableId(name), -2);
        assertEquals(Database.getCatalog().getTableId(""), -1);
        
        try {
            Database.getCatalog().getTableId(null);
            Assert.fail("Should not find table with null name");
        } catch (NoSuchElementException e) {
            // Expected to get here
        }
        
        try {
            Database.getCatalog().getTableId("foo");
            Assert.fail("Should not find table with name foo");
        } catch (NoSuchElementException e) {
            // Expected to get here
        }
    }

    /**
     * Unit test for Catalog.getDbFile()
     */
    @Test public void getDbFile() throws Exception {
        DbFile f = Database.getCatalog().getDbFile(-1);

        // NOTE(ghuo): we try not to dig too deeply into the DbFile API here; we
        // rely on HeapFileTest for that. perform some basic checks.
        assertEquals(-1, f.id());
    }

    /**
     * JUnit suite target
     */
    public static junit.framework.Test suite() {
        return new JUnit4TestAdapter(CatalogTest.class);
    }
}

