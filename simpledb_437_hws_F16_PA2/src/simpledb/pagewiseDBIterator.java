package simpledb;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * A wrapper iterator class which uses the input DbIterator
 * to iterate over the underlying DBFile page-wise.
 */
public class pagewiseDBIterator {

	private DbIterator dbIterator;
	
	public pagewiseDBIterator(DbIterator dbIterator) {
		this.dbIterator = dbIterator;
	}

	public boolean hasNext() throws DbException, TransactionAbortedException, IOException {
		return dbIterator.hasNext();
	}

	public HeapPage next() throws DbException, TransactionAbortedException, NoSuchElementException, IOException {
		return readNextPage(dbIterator);
	}

	public void open() throws DbException, TransactionAbortedException, IOException {
		dbIterator.open();
    }
	
	public void close() {
		dbIterator.close();
    }

    public void rewind() throws DbException, TransactionAbortedException, IOException {
		dbIterator.rewind();
	}
	
	private HeapPage readNextPage(DbIterator dbIterator) throws NoSuchElementException, DbException, TransactionAbortedException, IOException {
		HeapPage heapPage = null;
		if (dbIterator.hasNext()) {
			//creating a heap page object to temporarily store data
			heapPage = HeapPage.createTempHeapPage(dbIterator.getTupleDesc());
			int capacity = heapPage.getNumSlots();
			while (dbIterator.hasNext() && (capacity > 0)) {
				heapPage.addTuple(dbIterator.next());
				--capacity;
			}
		}
		
		return heapPage;
	}
}