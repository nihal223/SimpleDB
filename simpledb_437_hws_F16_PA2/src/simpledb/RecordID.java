package simpledb;

/**
 * A RecordID is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordID {

    PageId _pageid;
    int _tupleno;

    /** Constructor.
     * @param pid the pageid of the page on which the tuple resides
     * @param tupleno the tuple number within the page.
     */
    public RecordID(PageId pid, int tupleno) {
        _pageid=pid;
	_tupleno=tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int tupleno() {
          return _tupleno;
    }

    /**
     * @return the table id this RecordId references.
     */
    public PageId pageid() {
          return _pageid;
    }
}
