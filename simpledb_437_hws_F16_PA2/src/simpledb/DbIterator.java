package simpledb;
import java.util.*;
import java.io.*;

/**
 * DbIterator is the iterator interface that all SimpleDB operators should
 * implement.
 */
public interface DbIterator {
  /**
   * Opens the iterator.
   * @throws NoSuchElementException when the iterator has no elements.
   * @throws DbException when there are problems opening/accessing the database.
   */
  public void open()
      throws DbException, TransactionAbortedException, IOException;

  /** @return true if the iterator has more items. */
    public boolean hasNext() throws DbException, TransactionAbortedException, IOException;

  /**
   * Gets the next tuple from the operator (typically implementing by reading
   * from a child operator or an access method).
   *
   * @return The next tuple in the iterator, or null if there are no more tuples.

   */
  public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException;

  /**
   * Resets the iterator to the start.
   * @throws DbException When rewind is unsupported.
   */
    public void rewind() throws DbException, TransactionAbortedException, IOException;

  /**
   * Returns the TupleDesc associated with this DbIterator.
   */
  public TupleDesc getTupleDesc();

  /**
   * Closes the iterator.
   */
  public void close();

  //added
  /**
   * Seek the underlying iterator to a particular record.
   * @param record - Record Id for the tuple to be seeked in DB file.
   * @return The tuple with the input record ID.
   * @throws DbException
   * @throws TransactionAbortedException
   * @throws IOException
   */
  public Tuple seek(RecordID record) throws DbException, TransactionAbortedException, IOException;
}
