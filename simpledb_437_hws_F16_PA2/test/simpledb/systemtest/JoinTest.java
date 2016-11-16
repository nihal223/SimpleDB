package simpledb.systemtest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.junit.Test;

import simpledb.*;

       

public class JoinTest {
    private static final int COLUMNS = 4;
    public void validateJoin(int table1ColumnValue, int table1Rows, int table2ColumnValue,
            int table2Rows)
            throws IOException, DbException, TransactionAbortedException {
        // Create the two tables
        HashMap<Integer, Integer> columnSpecification = new HashMap<Integer, Integer>();
        columnSpecification.put(0, table1ColumnValue);
        ArrayList<ArrayList<Integer>> t1Tuples = new ArrayList<ArrayList<Integer>>();
        HeapFile table1 = SystemTestUtil.createRandomHeapFile(
                COLUMNS, table1Rows, columnSpecification, t1Tuples);
        assert t1Tuples.size() == table1Rows;
	System.out.println("JoinTest Relation 1 is  "+ table1.numPages() +"  pages");

        columnSpecification.put(0, table2ColumnValue);
        ArrayList<ArrayList<Integer>> t2Tuples = new ArrayList<ArrayList<Integer>>();
        HeapFile table2 = SystemTestUtil.createRandomHeapFile(
                COLUMNS, table2Rows, columnSpecification, t2Tuples);
        assert t2Tuples.size() == table2Rows;
	System.out.println("JoinTest Relation 2 is  "+ table2.numPages() +"  pages");

        // Generate the expected results
        ArrayList<ArrayList<Integer>> expectedResults = new ArrayList<ArrayList<Integer>>();
        for (ArrayList<Integer> t1 : t1Tuples) {
            for (ArrayList<Integer> t2 : t2Tuples) {
                // If the columns match, join the tuples
                if (t1.get(0).equals(t2.get(0))) {
                    ArrayList<Integer> out = new ArrayList<Integer>(t1);
                    out.addAll(t2);
                    expectedResults.add(out);
                }
            }
        }

        // Begin the join
        TransactionId tid = new TransactionId();
        SeqScan ss1 = new SeqScan(tid, table1.id(), "");
        SeqScan ss2 = new SeqScan(tid, table2.id(), "");
        JoinPredicate p = new JoinPredicate(0, Predicate.Op.EQUALS, 0);
        Join joinOp = new Join(p, ss1, ss2);
	joinOp.setJoinAlgorithm(Join.SNL);

        // test the join results
        SystemTestUtil.matchTuples(joinOp, expectedResults);

        joinOp.close();
        Database.getBufferPool().transactionComplete(tid);
        Database.getBufferPool().flushAllPages();
	System.out.println("Outer Relation: "+ss1.getPagesRead()+" pages read");
	System.out.println("Inner Relation: "+ss2.getPagesRead()+" pages read");
	System.out.println("Number of Joined Tuples: "+joinOp.getNumMatches());
    }

    //runs the join but does not check the results (to save memory)
    public void performJoin(int table1ColumnValue, int table1Rows, int table2ColumnValue,
            int table2Rows)
            throws IOException, DbException, TransactionAbortedException {
        // Create the two tables
        HashMap<Integer, Integer> columnSpecification = new HashMap<Integer, Integer>();
        columnSpecification.put(0, table1ColumnValue);
        ArrayList<ArrayList<Integer>> t1Tuples = new ArrayList<ArrayList<Integer>>();
        HeapFile table1 = SystemTestUtil.createRandomHeapFile(
                COLUMNS, table1Rows, columnSpecification, t1Tuples);
        assert t1Tuples.size() == table1Rows;
	System.out.println("JoinTest Relation 1 is  "+ table1.numPages() +"  pages");

        columnSpecification.put(0, table2ColumnValue);
        ArrayList<ArrayList<Integer>> t2Tuples = new ArrayList<ArrayList<Integer>>();
        HeapFile table2 = SystemTestUtil.createRandomHeapFile(
                COLUMNS, table2Rows, columnSpecification, t2Tuples);
        assert t2Tuples.size() == table2Rows;
	System.out.println("JoinTest Relation 2 is  "+ table2.numPages() +"  pages");

    

        // Begin the join
        TransactionId tid = new TransactionId();
        SeqScan ss1 = new SeqScan(tid, table1.id(), "");
        SeqScan ss2 = new SeqScan(tid, table2.id(), "");
        JoinPredicate p = new JoinPredicate(0, Predicate.Op.EQUALS, 0);
        Join joinOp = new Join(p, ss1, ss2);
	joinOp.setJoinAlgorithm(Join.SNL);

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
        validateJoin(1, 1, 1, 1);
    }

    @Test public void testNoMatch()
            throws IOException, DbException, TransactionAbortedException {
	System.out.println("--------------------------------------------");
	System.out.println("JoinTest - Test No Match");
        validateJoin(1, 2, 2, 10);
    }

    @Test public void testMultipleMatch()
            throws IOException, DbException, TransactionAbortedException {
	System.out.println("--------------------------------------------");
	System.out.println("JoinTest - Test Multiple Matches");
        validateJoin(1, 11, 1, 22);
    }

    @Test public void testLargeOuter()
          throws IOException, DbException, TransactionAbortedException {
    	System.out.println("--------------------------------------------");
    	System.out.println("JoinTest - Test Large Outer");
	performJoin(1, 700, 1, 1);
    }


    @Test public void testLargeInner()
        throws IOException, DbException, TransactionAbortedException {
    	System.out.println("--------------------------------------------");
    	System.out.println("JoinTest - Test Large Inner");
	performJoin(1, 1, 1, 700);
    }

    @Test public void testLargeOuterInner()
          throws IOException, DbException, TransactionAbortedException {
    	System.out.println("--------------------------------------------");
    	System.out.println("JoinTest - Test Large Outer and Inter");
	performJoin(1, 500, 1, 600);
    }


    /** Make test compatible with older version of ant. */
    public static junit.framework.Test suite() {
        return new junit.framework.JUnit4TestAdapter(JoinTest.class);
    }
}
