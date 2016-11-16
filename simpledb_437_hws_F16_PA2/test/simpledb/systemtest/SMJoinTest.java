package simpledb.systemtest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.junit.Test;

import simpledb.*;

       

public class SMJoinTest {
    private static final int COLUMNS = 4;
    //runs the join but does not check the results (to save memory)
    public void performSortedJoin(double table1Duplicate, int table1Rows, double table2Duplicate,
				  int table2Rows, boolean printTable)
            throws IOException, DbException, TransactionAbortedException {
        // Create the two tables
         ArrayList<ArrayList<Integer>> t1Tuples = new ArrayList<ArrayList<Integer>>();
        HeapFile table1 = SystemTestUtil.createSortedFile(
							  COLUMNS, table1Rows, table1Duplicate, t1Tuples, printTable);

        assert t1Tuples.size() == table1Rows;
	System.out.println("JoinTest Relation 1 is  "+ table1.numPages() +"  pages");


        ArrayList<ArrayList<Integer>> t2Tuples = new ArrayList<ArrayList<Integer>>();
        HeapFile table2 = SystemTestUtil.createSortedFile(
							  COLUMNS, table2Rows, table2Duplicate, t2Tuples, printTable);
        assert t2Tuples.size() == table2Rows;
	System.out.println("JoinTest Relation 2 is  "+ table2.numPages() +"  pages");
    

        // Begin the join
        TransactionId tid = new TransactionId();
        SeqScan ss1 = new SeqScan(tid, table1.id(), "");
        SeqScan ss2 = new SeqScan(tid, table2.id(), "");
        JoinPredicate p = new JoinPredicate(0, Predicate.Op.EQUALS, 0);
        Join joinOp = new Join(p, ss1, ss2);
	joinOp.setJoinAlgorithm(3);

        // create and drop the join results
        SystemTestUtil.countJoinTuples(joinOp);

        joinOp.close();
        Database.getBufferPool().transactionComplete(tid);
        Database.getBufferPool().flushAllPages();
	System.out.println("Outer Relation: "+ss1.getPagesRead()+" pages read");
	System.out.println("Inner Relation: "+ss2.getPagesRead()+" pages read");
	System.out.println("Number of Joined Tuples: "+joinOp.getNumMatches());

    }
    
    @Test public void testSingleMatch()
            throws IOException, DbException, TransactionAbortedException {
	System.out.println("--------------------------------------------");
	System.out.println("JoinTest - Test Single Match");
        performSortedJoin(1, 1, 1, 1, false);
    }
    
     
    @Test public void testMultipleMatch()
            throws IOException, DbException, TransactionAbortedException {
	System.out.println("--------------------------------------------");
	System.out.println("JoinTest - Test Multiple Matches");
	System.out.println("**** RESULTS WILL DIFFER, CHECK TABLE VALUES FOR CORRECTNESS ***");
        performSortedJoin(0.5, 6, 0.5, 6, true);
    }
    
    @Test public void testSmallCrossProduct()
          throws IOException, DbException, TransactionAbortedException {
    	System.out.println("--------------------------------------------");
    	System.out.println("JoinTest - Test CrossProduct");
	performSortedJoin(0, 10, 0, 10, false);
    }
    
    @Test public void testLargeCrossProduct()
          throws IOException, DbException, TransactionAbortedException {
    	System.out.println("--------------------------------------------");
    	System.out.println("JoinTest - Test CrossProduct");
	performSortedJoin(0, 600, 0, 600, false);
	}
    @Test public void testUniqueOuter()
          throws IOException, DbException, TransactionAbortedException {
    	System.out.println("--------------------------------------------");
    	System.out.println("JoinTest - Test Unique Outer");
	performSortedJoin(0, 600, 1, 600, false);
    }
    
    
    @Test public void testUniqueInner()
        throws IOException, DbException, TransactionAbortedException {
    	System.out.println("--------------------------------------------");
    	System.out.println("JoinTest - Test Unique Inner");
	performSortedJoin(1, 600, 0, 600, false);
    }
   
    @Test public void testMultipleOuterInner()
          throws IOException, DbException, TransactionAbortedException {
    	System.out.println("--------------------------------------------");
	System.out.println("**** RESULTS WILL DIFFER ***");
    	System.out.println("JoinTest - Test Large Outer and Inner - Random values");
	performSortedJoin(0.6, 600, 0.6, 600, false);
    }
    

    /** Make test compatible with older version of ant. */
    public static junit.framework.Test suite() {
        return new junit.framework.JUnit4TestAdapter(SMJoinTest.class);
    }
}
