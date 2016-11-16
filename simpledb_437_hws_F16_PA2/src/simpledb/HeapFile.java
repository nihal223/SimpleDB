package simpledb;

import java.io.*;
import java.util.*;
import java.text.ParseException;

/**
 * HeapFile is an implementation of a DbFile that stores a collection
 * of tuples in no particular order.  Tuples are stored on pages, each of
 * which is a fixed size, and the file is simply a collection of those
 * pages. HeapFile works closely with HeapPage.  The format of HeapPages
 * is described in the HeapPage constructor.
 *
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    File hfile;
    TupleDesc _td;
    /**
     * Constructor.
     * Creates a new heap file that stores pages in the specified buffer pool.
     *
     * @param f The file that stores the on-disk backing store for this DbFile.
     */
    public HeapFile(File f) {
        this.hfile = f;
    }

    public HeapFile(File f, TupleDesc td) {
        this.hfile = f;
	this._td = td;
    }

    /**
     * Return a Java File corresponding to the data from this HeapFile on disk.
     */
    public File getFile() {
        return hfile;
    }

    /**
     * @return an ID uniquely identifying this HeapFile
     *  (Implementation note:  you will need to generate this tableid somewhere,
     *    ensure that each HeapFile has a "unique id," and that you always
     *    return the same value for a particular HeapFile.  The implementation we
     *    suggest you use could hash the absolute file name of the file underlying
     *    the heapfile, i.e. f.getAbsoluteFile().hashCode()
     *    )
     */
    public int id() {
	return this.hfile.getAbsoluteFile().hashCode();
    }

    /**
     * Returns a Page from tHhe file.
     */
    public Page readPage(PageId pid) throws NoSuchElementException, IOException, FileNotFoundException {
	//System.out.println("read hf page" + pid.tableid() +"  "+pid.pageno());
     
	byte [] readpage = new byte[BufferPool.PAGE_SIZE];
	for(int i=0; i<BufferPool.PAGE_SIZE; i++)
	    readpage[i]=0;
	DataInputStream buffer = new DataInputStream(new FileInputStream(hfile));
	
	int tuplesPerPage = (BufferPool.PAGE_SIZE*8) / ((_td.getSize()*8)+1);
	//	int headersize = (int)Math.ceil(tuplesPerPage/8);


	try {
	    //The headeer size may not be right
	    int sk = buffer.skipBytes((BufferPool.PAGE_SIZE)*pid.pageno());
	    if(sk!=-1){
		int c = buffer.read(readpage,0,BufferPool.PAGE_SIZE);
		if (c!=-1){
		    if (c!=BufferPool.PAGE_SIZE)
			readpage[c]=-1;
		    HeapPageId newPageId = (HeapPageId) pid;
		    return new HeapPage(newPageId,readpage);
		}
		else
		    return null;
	    }
	    else
		return null;
	} catch (IOException e) {
	    return null;
	} catch (NoSuchElementException e){
	    return null;
	}
    }

    /**
     * Writes the given page to the appropriate location in the file.
     */
    public void writePage(Page page) throws IOException {
        // some code goes here
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {

       	int tuplesPerPage = (BufferPool.PAGE_SIZE*8) / ((_td.getSize()*8)+1);
      	int pageCount = (int) (Math.ceil(this.hfile.length() / (double)(BufferPool.PAGE_SIZE)));        
	//	System.out.println(tuplesPerPage*_td.getSize()+"    "+this.hfile.length()+":"+tuplesPerPage+":"+_td.getSize()+">>>> "+pageCount);


    	return pageCount;

    }

    /**
     * Adds the specified tuple to the table under the specified TransactionId.
     *
     * @throws DbException
     * @throws IOException
     * @return An ArrayList contain the pages that were modified
     */
    public ArrayList<Page> addTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
  
    }

    /**
     * Deletes the specified tuple from the table, under the specified
     * TransactionId.
     */
    public Page deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
        // some code goes here
        return null;
  
    }

    /**
     * An iterator over all tuples on this file, over all pages.
     * Note that this iterator should use BufferPool.getPage(), rather than HeapFile.getPage()
     * to iterate through pages.
     */
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid,this);
    }

    /**
     * @return the number of bytes on a page, including the number of bytes
     * in the header.
     */
    public int bytesPerPage() {
        // some code goes here
        return 0;
    }

    public TupleDesc getTupleDesc(){
	return _td;
    }
    
}

