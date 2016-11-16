package simpledb;

import java.io.*;

/** Database is a class that initializes several static
    variables used by the database system (the catalog, the buffer pool,
    and the log files, in particular.)
    <p>
    Provides a set of methods that can be used to access these global variables
    from anywhere.
*/

public class Database {
    private static Catalog _catalog = new Catalog();
    private static BufferPool _bufferpool = new BufferPool(BufferPool.DEFAULT_PAGES);

    private Database() {
    }

    /** Return the static instance of the buffer pool */
    public static BufferPool getBufferPool() {
        return _bufferpool;
    }

    /** Return the static instance of the catalog */
    public static Catalog getCatalog() {
        return _catalog;
    }

    /** Method used for testing -- create a new instance of the
        buffer pool and return it
    */
    public static BufferPool resetBufferPool(int pages) {
        _bufferpool = new BufferPool(pages);
        return _bufferpool;
    }

    //Dummy function so recovery code compiles;  not needed in labs 1-3
    public static LogFile resetLogFile() throws IOException { return null; }

}
