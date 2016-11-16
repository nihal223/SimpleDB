package simpledb;

import java.io.*;

/**
 * Transaction encapsulates information about the state of
 * a transaction and manages transaction commit / abort.
 */

public class Transaction {
    TransactionId tid;
    boolean started = false;

    public Transaction() {
        tid = new TransactionId();
    }

    /** Start the transaction running */
    public void start() {
        started = true;
        // some code goes here
    }

    public TransactionId tid() {
        return tid;
    }

    /** Finish the transaction */
    public void commit() throws IOException {
        transactionComplete(false);
    }

    /** Handle the details of transaction commit / abort */
    public void transactionComplete(boolean abort) throws IOException {
        // some code goes here
    }

}
