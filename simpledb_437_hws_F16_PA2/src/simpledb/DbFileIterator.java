package simpledb;
import java.util.*;
import java.io.*;

/**
 * DbFileIterator is the iterator interface that all SimpleDB Dbfile should
 * implement.
 */
public interface DbFileIterator {
    /**
     * Opens the iterator.
     * @throws DbException when there are problems opening/accessing the database.
     */
    public void open()
        throws DbException, TransactionAbortedException,IOException;

    /** @return true if there are more tuples available. */
    public boolean hasNext()
        throws DbException, TransactionAbortedException, IOException;

    /**
     * Gets the next tuple from the operator (typically implementing by reading
     * from a child operator or an access method).
     *
     * @return The next tuple in the iterator.
     * @throws NoSuchElementException if there are no more tuples
     */
    public Tuple next()
        throws DbException, TransactionAbortedException, NoSuchElementException;

    /**
     * Resets the iterator to the start.
     * @throws DbException When rewind is unsupported.
     */
    public void rewind() throws DbException, TransactionAbortedException, IOException;

    /**
     * Closes the iterator.
     */
    public void close();

    public int getPagesRead();

    public Tuple seek(RecordID record) throws DbException, TransactionAbortedException, IOException;
}
