package simpledb.systemtest;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;

import org.junit.Test;

import junit.framework.Assert;
import simpledb.*;

public class PinCountTest {
    private static final long MEMORY_LIMIT_IN_MB = 2;
    private static final int BUFFER_PAGES = 5;
    public static final int DEFAULT_POLICY = 0;
    public static final int LRU_POLICY = 1;
    public static final int MRU_POLICY = 2;
    
    
    @Test public void testPinCount() throws IOException, DbException, TransactionAbortedException {
	HeapPageId pid;
	Page page;

        System.out.println("testPinCount creating large table");
        HeapFile f = SystemTestUtil.createRandomHeapFile(2, 1024*5, null, null);
        System.out.println("testPinCount File Size is  ***"+ f.numPages() +"***  pages");
       

        System.out.println("-------------------------------------------------------------------"); 
        
        System.out.println("1 - testPinCount testing LRU");
        Database.resetBufferPool(BUFFER_PAGES);
        System.out.println("New Buffer, Size " + BUFFER_PAGES +" pages");
        Database.getBufferPool().setReplacePolicy(LRU_POLICY);
        
        System.out.println("Sequential scan of file ID: "+f.id());
        SeqScan scan = new SeqScan(null, f.id(), "");  
        scan.open();
        boolean once = true;
        BufferPool buffer = Database.getBufferPool();
    	while (scan.hasNext()) {
    		scan.next();
    		if ((scan.getPagesRead() == 5) && (once)) {
    			once = false;
    	    	//buffer = Database.getBufferPool();
    	    	buffer.printBufferPool();
    	    	try {
					buffer.unpinPage(0, null, false);
					buffer.unpinPage(4, null, false);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
    	    	
    	    	//buffer.printBufferPool();
    		}
    		if (scan.getPagesRead() == 7) {
    			buffer.printBufferPool();
    			break;
    		}
    	}
    	scan.close();        
        
        System.out.println("-------------------------------------------------------------------"); 
        
        System.out.println("2 - testPinCount testing MRU");
        Database.resetBufferPool(BUFFER_PAGES);
        System.out.println("New Buffer, Size " + BUFFER_PAGES +" pages");
        Database.getBufferPool().setReplacePolicy(MRU_POLICY);
        
        scan.open();
        once = true;
    	while (scan.hasNext()) {
    		scan.next();
    		if ((scan.getPagesRead() == 5) && (once)) {
    			once = false;
    	    	buffer = Database.getBufferPool();
    	    	buffer.printBufferPool();
    	    	try {
					buffer.unpinPage(0, null, false);
					buffer.unpinPage(4, null, false);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
    	    	
    	    	//buffer.printBufferPool();
    		}
    		if (scan.getPagesRead() == 7) {
    			buffer.printBufferPool();
    			break;
    		}
    	}
    	scan.close();
    	
    	System.out.println("-------------------------------------------------------------------"); 
    	
        System.out.println("3 - testPinCount testing LRU");
        Database.resetBufferPool(BUFFER_PAGES);
        System.out.println("New Buffer, Size " + BUFFER_PAGES +" pages");
        Database.getBufferPool().setReplacePolicy(LRU_POLICY);
        
        buffer = Database.getBufferPool();

    	for (int i=0;i<2;i++){
    	    scan.open();
    	    while (scan.hasNext()) {
    	    	scan.next();
    	    	if (scan.getPagesRead() == 5) {
    	    		once = false;
    	    		break;
    	    	}
    	    }
    	    scan.close();
    	}    	    	
        once = true;    	
        scan.open();
        while (scan.hasNext()) {
        	scan.next();
        	if ((scan.getPagesRead() == 5) && (once)) {
        		once = false;
        		try {
					buffer.unpinPage(1, null, false);
	        		buffer.unpinPage(3, null, false);
	        		buffer.unpinPage(2, null, false);
	        		buffer.unpinPage(1, null, false);
	        		buffer.unpinPage(3, null, false);
	        		buffer.unpinPage(2, null, false);    	    		
	        		buffer.unpinPage(2, null, false);
	        		buffer.unpinPage(1, null, false);
	        		buffer.unpinPage(3, null, false);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

        		buffer.printBufferPool();
        	}
        	if (scan.getPagesRead() == 8) {
        		buffer.printBufferPool();
        		break;
        	}    	    	
        }
        scan.close();
    	      	
    	System.out.println("-------------------------------------------------------------------"); 
    	
        System.out.println("4 - testPinCount testing MRU");
        Database.resetBufferPool(BUFFER_PAGES);
        System.out.println("New Buffer, Size " + BUFFER_PAGES +" pages");
        Database.getBufferPool().setReplacePolicy(MRU_POLICY);
        
        buffer = Database.getBufferPool();

    	for (int i=0;i<2;i++){
    	    scan.open();
    	    while (scan.hasNext()) {
    	    	scan.next();
    	    	if (scan.getPagesRead() == 5) {
    	    		break;
    	    	}
    	    }
    	    scan.close();
    	}    	    	
        once = true;    	
        scan.open();
        while (scan.hasNext()) {
        	scan.next();
        	if ((scan.getPagesRead() == 5) && (once)) {
        		once = false;
        		try {
					buffer.unpinPage(1, null, false);
	        		buffer.unpinPage(3, null, false);
	        		buffer.unpinPage(2, null, false);
	        		buffer.unpinPage(1, null, false);
	        		buffer.unpinPage(3, null, false);
	        		buffer.unpinPage(2, null, false);    	    		
	        		buffer.unpinPage(2, null, false);
	        		buffer.unpinPage(1, null, false);
	        		buffer.unpinPage(3, null, false);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

        		buffer.printBufferPool();
        	}
        	if (scan.getPagesRead() == 8) {
        		buffer.printBufferPool();
        		break;
        	}    	    	
        }
        scan.close();
        
        
    	System.out.println("-------------------------------------------------------------------"); 
    	
        System.out.println("5 - testPinCount testing dirty_bit");
        Database.resetBufferPool(BUFFER_PAGES);
        System.out.println("New Buffer, Size " + BUFFER_PAGES +" pages");
        Database.getBufferPool().setReplacePolicy(MRU_POLICY);
        
        buffer = Database.getBufferPool();        
	    scan.open();
	    while (scan.hasNext()) {
	    	scan.next();
	    	if (scan.getPagesRead() == 5) {
	    		break;
	    	}
	    }
	    scan.close();
	    buffer.printBufferPool();
	    System.out.println();
	    
	    System.out.println("Unpining page from index 3, dirty_bit == true");
	    try {
			buffer.unpinPage(3, null, true);
		} catch (Exception e3) {
			// TODO Auto-generated catch block
			e3.printStackTrace();
		}
	    buffer.printBufferPool();
	    System.out.println();
	    
	    scan.open();
	    while (scan.hasNext()) {
	    	scan.next();
	    	if (scan.getPagesRead() == 5) {
	    		break;
	    	}
	    }
	    scan.close();
	    buffer.printBufferPool();
	    System.out.println();	    
	    
	    System.out.println("Unpining page from index 3, dirty_bit == false");
	    try {
			buffer.unpinPage(3, null, false);
		} catch (Exception e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
	    buffer.printBufferPool();
	    System.out.println();
	    
	    System.out.println("Unpining page from index 4, dirty_bit == true");
	    try {
			buffer.unpinPage(4, null, true);
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	    buffer.printBufferPool();
	    System.out.println();   
	    	    
    	System.out.println("-------------------------------------------------------------------"); 
    	
        System.out.println("6 - testPinCount testing error while trying to unpin a page");
        Database.resetBufferPool(BUFFER_PAGES);
        System.out.println("New Buffer, Size " + BUFFER_PAGES +" pages");
        Database.getBufferPool().setReplacePolicy(LRU_POLICY);
        
        scan.open();
    	while (scan.hasNext()) {
    		scan.next();
    		if (scan.getPagesRead() == 5) {
    			once = false;
    	    	buffer = Database.getBufferPool();
    	    	try {
        	    	buffer.unpinPage(0, null, false);
        	    	buffer.unpinPage(4, null, false);
        	    	buffer.unpinPage(0, null, false);
					buffer.unpinPage(3, null, false);
				} catch (DbException e) {
					// TODO Auto-generated catch block
		        	System.out.println(e);
				}
    	    	break;
    		}
    	}
    	scan.close();
	    System.out.println();   

    	
    	System.out.println("-------------------------------------------------------------------"); 

    	
        System.out.println("7 - testPinCount testing full buffer pool");
        Database.resetBufferPool(BUFFER_PAGES);
        System.out.println("New Buffer, Size " + BUFFER_PAGES +" pages");
        Database.getBufferPool().setReplacePolicy(MRU_POLICY);
    	
        try {
        	scan.open();
        	while (scan.hasNext()) {
        		scan.next();
        	}
        } catch (DbException e) {
        	System.out.println(e);
        }
	    
    }  
    
    
    
    /** Make test compatible with older version of ant. */
    public static junit.framework.Test suite() {
        return new junit.framework.JUnit4TestAdapter(PinCountTest.class);
    }    

}
