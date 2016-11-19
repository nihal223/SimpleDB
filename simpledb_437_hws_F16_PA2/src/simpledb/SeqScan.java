package simpledb;
import java.util.*;
import java.io.*;

import java.io.IOException;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    TransactionId _tid;
    int _tableId;
    String _tableAlias;
    DbFileIterator _iterator;
    DbFile _file;

    /**
     * Constructor.
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid The transaction this scan is running as a part of.
     * @param tableid the table to scan.
     * @param tableAlias the alias of this table (needed by the parser);
     *         the returned tupleDesc should have fields with name tableAlias.fieldName
     *         (note: this class is not responsible for handling a case where tableAlias
     *         or fieldName are null.  It shouldn't crash if they are, but the resulting
     *         name can be null.fieldName, tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        _tid = tid;
	_tableId = tableid;
	_tableAlias = tableAlias;
	_file = Database.getCatalog().getDbFile(_tableId);

	assert(_file != null);
    }

    /**
     * Opens this sequential scan.
     * Needs to be called before getNext().
     */
    public void open()
        throws DbException, TransactionAbortedException, IOException {
        HeapFile heapFile = (HeapFile) _file;
	//System.out.println("open file "+_file.id()+" with pages "+heapFile.numPages());
	assert (heapFile != null);
	_iterator = new HeapFileIterator(_tid,heapFile);
	_iterator.open();
    }

    /**
     * Implementation of DbIterator.getTupleDesc method.
     * Should return a tupleDesc with field names from the underlying HeapFile with field
     *   names prefaced by the passed in tableAlias string
     */
    public TupleDesc getTupleDesc() {
        TupleDesc schema = _file.getTupleDesc();
	int length = schema.numFields();

	Type[] types = new Type[length];
	String[] names = new String[length];
	for (int i=0; i<length; i++) {
	    types[i] = schema.getType(i);
	    names[i] = _tableAlias +"."+schema.getFieldName(i);
	}
	return new TupleDesc(types,names);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException, IOException {
        // some code goes here
        return _iterator.hasNext();
    }

    /**
     * Implementation of DbIterator.getNext method.
     * Return the next tuple in the scan, or null if there are no more tuples.
     *
     */
    public Tuple next()
        throws NoSuchElementException, TransactionAbortedException, DbException {
    
        return _iterator.next();
    }
    public Tuple previous()
        throws NoSuchElementException, TransactionAbortedException, DbException, IOException {
    
        return ((HeapFileIterator)_iterator).previous();
    }

    /**
     * Closes the sequential scan.
     */
    public void close() {
	_iterator.close();
    }

    public int getPagesRead(){
	return _iterator.getPagesRead();
    }


    /**
     * Rewinds the sequential back to the first record.
     */
    public void rewind()
        throws DbException, NoSuchElementException, TransactionAbortedException, IOException {
        _iterator.rewind();
    }

    public DbFileIterator getIterator(){
	return _iterator;
    }

    /* (non-Javadoc)
     * @see simpledb.DbIterator#goToRecord(simpledb.RecordID)
     */
    @Override
    public Tuple goToRecord(RecordID record) throws DbException, TransactionAbortedException, IOException {
        return _iterator.goToRecord(record);
    }
}
