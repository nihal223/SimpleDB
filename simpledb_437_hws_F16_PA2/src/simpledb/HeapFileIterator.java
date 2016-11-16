
package simpledb;

import java.util.*;
import java.io.*;

public class HeapFileIterator implements DbFileIterator {
    private TransactionId _transactionId;
    private HeapFile _file;
    private int _currentPageId;
    private Page _currentPage;
    private int _pagesRead =0;
    private int _numPages;
    private Iterator<Tuple> _tupleIterator;

    public HeapFileIterator(TransactionId tid, HeapFile file) {
        _transactionId = tid;
        _file = file;
        _currentPageId = 0;
        _numPages = _file.numPages();
	//			System.out.println("numpages "+_numPages);
    }

    public void open()
        throws DbException, TransactionAbortedException, IOException {
	
        _currentPage = readPage(_currentPageId++);
	 _pagesRead++;
        _tupleIterator = _currentPage.iterator();
    }

    public boolean hasNext()
        throws DbException, TransactionAbortedException, IOException {
	//System.out.println("hf has next: " + _currentPageId +_numPages);
        if (_tupleIterator == null) return false;
	if (_tupleIterator.hasNext()) return true;

        // If we have more pages
	//System.out.println(_currentPageId+"   llllll   "+_numPages);
        while (_currentPageId < (_numPages)) {
        	_currentPage = readPage(_currentPageId++);
        	_tupleIterator = _currentPage.iterator();
        	if (_tupleIterator.hasNext()) {
		        _pagesRead++;
        		return true;
        	}
        } 
        
        return false;
    }

    //added
    public Tuple seek(RecordID record) throws TransactionAbortedException, IOException, DbException {
        Tuple tupleSeeked = null;
        if (record.pageid().pageno() >= _numPages) {
            throw new NoSuchElementException("Invalid page id for record");
        } 

        if (((_currentPageId == 0) && (_currentPageId != record.pageid().pageno()))
                || (record.pageid().pageno() != (_currentPageId - 1))) {
            _currentPage = readPage(record.pageid().pageno());
            _currentPageId = record.pageid().pageno() + 1;
            _pagesRead++;
        }

        _tupleIterator = _currentPage.iterator();
        
        Tuple tmp = null;
        while (_tupleIterator.hasNext()) {
            tmp = _tupleIterator.next();
            if (tmp.getRecordID().equals(record)) {
                tupleSeeked = tmp;
                break;
            }
        }
        
        if (tupleSeeked == null) {
            throw new NoSuchElementException("Invalid Record id for record");
        }
        
        return tupleSeeked;
    }

    public Tuple next()
        throws DbException, TransactionAbortedException {
        if (_tupleIterator == null) {
            throw new NoSuchElementException("Tuple iterator not opened");
        }
        
        assert (_tupleIterator.hasNext());
        return _tupleIterator.next();
    }

    public Tuple previous()
        throws DbException, TransactionAbortedException, IOException {
	//	System.out.println("previous ");
        if (_tupleIterator == null) {
            throw new NoSuchElementException("Tuple iterator not opened");
        }
        
        if (((HeapPageIterator)_tupleIterator).hasPrevious()){
	    return ((HeapPageIterator)_tupleIterator).previous();
	}
	else {
	    if(_currentPageId > 1) {
		//System.out.println("CUURENT PAGEID "+_currentPageId);
		_currentPageId = _currentPageId-2;
		_currentPage = readPage(_currentPageId++);
		//System.out.println("NOW "+_currentPageId);
		_pagesRead++;
        	_tupleIterator = _currentPage.iterator();
        	((HeapPageIterator)_tupleIterator).setLast();
		if (((HeapPageIterator)_tupleIterator).hasPrevious()){
		    return ((HeapPageIterator)_tupleIterator).previous();
		}
	    }
	    else
		return null;
		
	}
	return null;
    }

    public void rewind()
        throws DbException, TransactionAbortedException, IOException {
        close();
        open();
    }

    public void close() {
        _currentPageId = 0;
        _tupleIterator = null;
    }

    public int getPagesRead(){
	return _pagesRead;
    }

    private Page readPage(int pageNumber) 
    	throws DbException, TransactionAbortedException, IOException {
        // File == table because we do one file per table
	//	System.out.println("readpage:"+_file.id()+" page:"+pageNumber);
        int tableId = _file.id();
        int pageId = pageNumber;
	//	System.out.println("Page is now "+pageNumber);
        HeapPageId pid = new HeapPageId(tableId, pageId);
       return Database.getBufferPool().getPage(_transactionId, pid, Permissions.READ_ONLY);
    }

    public Page getCurrentPage(){
	return _currentPage;
    }
}
