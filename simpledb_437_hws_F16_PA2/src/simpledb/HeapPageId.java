package simpledb;

/** Unique identifier for HeapPage objects. */
public class HeapPageId implements PageId {

    int _tableId;
    int _pgNo;

    /**
     * Constructor. Create a page id structure for a specific page of a
     * specific table.
     *
     * @param tableId The table that is being referenced
     * @param pgNo The page number in that table.
     */
    public HeapPageId(int tableId, int pgNo) {
        this._tableId=tableId;
	this._pgNo=pgNo;
    }

    /** @return the table associated with this PageId */
    public int tableid() {
        return this._tableId;
    }

    /**
     * @return the page number in the table tableid() associated with
     *   this PageId
     */
    public int pageno() {
	return this._pgNo;
    }

    /**
     * @return a hash code for this page, represented by the concatenation of
     *   the table number and the page number (needed if a PageId is used as a
     *   key in a hash table in the BufferPool, for example.)
     * @see BufferPool
     */
    public int hashCode() {
	int mask = 0x0000FFFF;
        int hash = (_tableId & mask) << 16;
	hash = hash | (_pgNo & mask);
        return hash;

    }

    /**
     * Compares one PageId to another.
     *
     * @param o The object to compare against (must be a PageId)
     * @return true if the objects are equal (e.g., page numbers and table
     *   ids are the same)
     */
    public boolean equals(Object o) {
	try {
	    PageId pid = (PageId) o;
	    return this.hashCode()==pid.hashCode();
	} catch (Exception e) {
	    return false;
	}
    }

    /**
     *  Return a representation of this object as an array of
     *  integers, for writing to disk.  Size of returned array must contain
     *  number of integers that corresponds to number of args to one of the
     *  constructors.
     */
    public int[] serialize() {
        int data[] = new int[2];

	data[0] = this.tableid();
	data[1] = this.pageno();
        return data;
    }

}
