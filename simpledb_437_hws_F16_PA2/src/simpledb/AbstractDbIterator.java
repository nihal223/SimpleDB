package simpledb;

import java.util.NoSuchElementException;
//added
import java.io.IOException;

/** Helper for implementing DbIterators. Handles hasNext()/next() logic and
 *  throwing exceptions if open()/close() are abused. */
public abstract class AbstractDbIterator implements DbIterator {
    //    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (next == null) next = readNext();
        return next != null;
    }

    //    @Override
    public Tuple next() throws
            DbException, TransactionAbortedException, NoSuchElementException {
        if (next == null) {
            next = readNext();
            if (next == null) throw new NoSuchElementException();
        }

        Tuple result = next;
        next = null;
        return result;
    }

    /** @return the next Tuple in the iterator, null if the iteration is finished. */
    protected abstract Tuple readNext() throws DbException, TransactionAbortedException;

    /** If subclasses override this, they should call super.close(). */
    //    @Override
    public void close() {
        // Ensures that a future call to next() will fail
        next = null;
    }

    //added
    /* (non-Javadoc)
     * @see simpledb.DbIterator#goToRecord(simpledb.RecordID)
     */
    @Override
    public Tuple goToRecord(RecordID record) throws DbException, TransactionAbortedException, IOException {
        throw new DbException("Not a supported operation");
    }

    private Tuple next = null;
}
