package simpledb;

import java.util.*;
import java.io.*;

/**
 * HeapPage stores pages of HeapFiles and implements the Page interface that is
 * used by BufferPool.
 * 
 * November 20, 2015-
 * Now we not only use it in BufferPool but but also for temporary storage of tuples.
 * Needed it for page wise iteration in page nested loop join.
 *
 * @see HeapFile
 * @see BufferPool
 */
public class HeapPage implements Page {

    HeapPageId pid;
    TupleDesc td;
    Tuple tuples[];
    int numSlots;
    boolean dirty;
    TransactionId lasttrans;
    int pin_count;
    Header header;

    /**
     * Abstracting the representation and implementation of header data, such that
     * the outside caller only knows about slots and bytes. Internally we represent it in the form of an int[].
     */
    class Header { 
        int[] header;
        
        /**
         * @param dis - Data input stream to be used to fetch header data.
         */
        public Header(DataInputStream dis) {
            
            // We are allocating 1 bit per slot, storing data for 32 slots in one integer.
            int numHeaderInts = (int)Math.ceil((float)numSlots/32);
            //representing in the form of int[]
            header = new int[numHeaderInts];
            
            try {
                for (int index = 0; index < numHeaderInts; ++index) {
                    header[index] = dis.readInt();
                }               
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        /**
         * Get the byte[] format for header structure.
         * @return
         * @throws IOException
         */
        public byte[] getHeader() throws IOException {

            ByteArrayOutputStream baos = new ByteArrayOutputStream(length());
            DataOutputStream dos = new DataOutputStream(baos);
            for (int index = 0; index < header.length; ++index) {
                dos.writeInt(header[index]);
            }
            
            byte[] headerData = baos.toByteArray();
            dos.close();
            
            return headerData;
        }
        
        /**
         * @return - The maximum length of header in bytes.
         */
        public int length() {
            return (header.length * 4);
        }
        
        /**
         * Get the header bit for the slot index.
         * @param slotIndex - Slot index in the heap file.
         * @return - Slot's header bit.
         */
        public boolean getSlotVal(int slotIndex) {
            int bitValue = (header[getHeaderIndex(slotIndex)] & (1<<getBitIndex(slotIndex)));
            return (bitValue != 0);
        }
        
        /**
         * Set the value for the given slot index.
         * @param slotIndex - Slot index in the heap file.
         * @param value - Boolean value for the slot.
         */
        public void setSlotVal(int slotIndex, boolean value) {
            int bitIndex = getBitIndex(slotIndex);
            int bitMask = value?(1<<bitIndex):(~(1<<bitIndex));
            
            if (value) {
                header[getHeaderIndex(slotIndex)] = (header[getHeaderIndex(slotIndex)] | bitMask);
            } else {
                header[getHeaderIndex(slotIndex)] = (header[getHeaderIndex(slotIndex)] & bitMask);
            }
        }

        /**
         * find all slots with the given value.
         * 
         * @param slotValue - Slot value to be used to search.
         * @return - Indexes of all found slots.
         */
        public List<Integer> findSlots(boolean slotValue) {
            List<Integer> searchResults = new ArrayList<Integer>();
            for (int index = 0; index < numSlots; ++index) {
                if (getSlotVal(index) == slotValue) {
                    searchResults.add(index);
                }
            }
            
            return searchResults;
        }
        
        /**
         * @return the index of first empty slot, -1 if no slot available
         */
        public int findFirstEmptySlot() {
            for (int index = 0; index < numSlots; ++index) {
                if (!getSlotVal(index)) {
                    return index;
                }
            }
            return -1;
        }

        /**
         * Given a slot ID, find its bit index (among 32 bits).
         * @param slotIndex- Slot's index in heap file.
         * @return - Bit index among the 32 bits.
         */
        private int getBitIndex (int slotIndex) {
            return (slotIndex % 32);
        }
        
        /**
         * Given a slot ID what's its corresponding header index?
         * @param slotIndex - Slot's index in heap file.
         * @return - Header index.
         */
        private int getHeaderIndex (int slotIndex) {
            return (slotIndex / 32);
        }
        
    }
    
    /**
     * Create a HeapPage from a set of bytes of data read from disk. The format
     * of a HeapPage is a set of 32-bit header words indicating the slots of the
     * page that are in use, plus (BufferPool.PAGE_SIZE/tuple size) tuple slots,
     * where tuple size is the size of tuples in this database table, which can
     * be determined via {@link Catalog#getTupleDesc}.
     *
     * The number of 32-bit header words is equal to:
     * <p>
     * (no. tuple slots / 32) + 1
     * <p>
     * 
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#PAGE_SIZE
     */
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.pid = id;
        this.td = Database.getCatalog().getTupleDesc(id.tableid());
        this.numSlots = (BufferPool.PAGE_SIZE * 8) / ((td.getSize() * 8) + 1);
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // allocate and read the header slots of this page
        header = new Header(dis);
        
        try {
            // allocate and read the actual records of this page
            tuples = new Tuple[numSlots];
            for (int i = 0; i < numSlots; i++) {
                tuples[i] = readNextTuple(dis, i);
            }
        } catch (NoSuchElementException e) {
            // e.printStackTrace();
        }
        
        dis.close();

        // initialize pin_count and dirty_bit
        this.pin_count = 0;
        this.dirty = false;
    }

    /**
     * Helper constructor to create a heap page to be used as buffer.
     * @param data - Data to be put in tmp page
     * @param td - Tuple description for the corresponding table.
     * @throws IOException
     */
    private HeapPage(byte[] data, TupleDesc td) throws IOException {
        this.pid = null; // No need of a pid here as this page serves as a dummy buffer page and is meant to be used independent of BufferManager.
        this.td = td;
        this.numSlots = (BufferPool.PAGE_SIZE * 8) / ((td.getSize() * 8) + 1);
        
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));
        this.header = new Header(dis);

        try {
            // allocate and read the actual records of this page
            tuples = new Tuple[numSlots];
            for (int i = 0; i < numSlots; i++) {
                tuples[i] = readNextTuple(dis, i);
            }
        } catch (NoSuchElementException e) {
            e.printStackTrace();
        }

        dis.close();

        // initialize pin_count and dirty_bit
        this.pin_count = 0;
        this.dirty = false;
    }
    
    /**
     * Return a view of this page before it was modified -- used by recovery
     */
    public HeapPage getBeforeImage() {
        // do not need to implement this

        return null;
    }

    /**
     * @return the PageId associated with this page.
     */
    public HeapPageId id() {
        return this.pid;

    }

    public int pin_count() {
        return this.pin_count;
    }

    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // if associated bit is not set, read forward to the next tuple, and
        // return null.
        if (!getSlot(slotId)) {
            for (int i = 0; i < td.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        Tuple t = new Tuple(td);
        RecordID rid = new RecordID(pid, slotId);
        t.setRecordID(rid);
        try {
            for (int j = 0; j < td.numFields(); j++) {
                Field f = td.getType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            // e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * Generates a byte array representing the contents of this page. Used to
     * serialize this page to disk.
     * <p>
     * The invariant here is that it should be possible to pass the byte array
     * generated by getPageData to the HeapPage constructor and have it produce
     * an identical HeapPage object.
     *
     * @see #HeapPage
     * @return A byte array correspond to the bytes of this page.
     */
    public byte[] getPageData() {
        // int len = header.length*4 + BufferPool.PAGE_SIZE;
        int len = BufferPool.PAGE_SIZE;
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // create the header of the page
        try {
            dos.write(header.getHeader());
        } catch (IOException e) {
            // this really shouldn't happen
            e.printStackTrace();
        }

        // create the tuples
        for (int i = 0; i < numSlots; i++) {

            // empty slot
            if (!getSlot(i)) {
                for (int j = 0; j < td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                continue;
            }

            // non-empty slot
            for (int j = 0; j < td.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zerolen = BufferPool.PAGE_SIZE - numSlots * td.getSize() - header.length();
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Static method to generate a byte array corresponding to an empty
     * HeapPage. Used to add new, empty pages to the file. Passing the results
     * of this method to the HeapPage constructor will create a HeapPage with no
     * valid tuples in it.
     *
     * @param tableid
     *            The id of the table that this empty page will belong to.
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData(int tableid) {
        TupleDesc td = Database.getCatalog().getTupleDesc(tableid);
        // int hb = (((BufferPool.PAGE_SIZE / td.getSize()) / 32) +1) * 4;
        int len = BufferPool.PAGE_SIZE;// + hb;
        return new byte[len]; // all 0
    }

    /**
     * @param td - Table description data for the corresponding table.
     * @return A heap Page for storing page data temporarily.
     * @throws IOException
     */
    public static HeapPage createTempHeapPage(TupleDesc td) throws IOException {
        return new HeapPage(new byte[BufferPool.PAGE_SIZE], td);
    }

    /**
     * Delete the specified tuple from the page.
     * 
     * @throws DbException
     *             if this tuple is not on this page, or tuple slot is already
     *             empty.
     * @param t
     *            The tuple to delete
     */
    public boolean deleteTuple(Tuple t) throws DbException {
        // no need to implement this
        return false;
    }

    /**
     * Adds the specified tuple to the page.
     * 
     * @throws DbException
     *             if the page is full (no empty slots) or tupledesc is
     *             mismatch.
     * @param t
     *            The tuple to add.
     */
    public void addTuple(Tuple t) throws DbException {
        int index = header.findFirstEmptySlot();
        if (index == -1) {
            throw new DbException("No empty slot for new tuple");
        } else {
            header.setSlotVal(index, true);
            tuples[index] = t;
        }
    }

    /**
     * Marks this page as dirty/not dirty and record that transaction that did
     * the dirtying
     */
    public void markDirty(boolean dirty, TransactionId tid) {
        this.dirty = dirty;
        this.lasttrans = tid;
    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null
     * if the page is not dirty
     */
    public TransactionId isDirty() {
        if (this.dirty == true)
            return this.lasttrans;
        else
            return null;
    }

    /**
     * Increment pin_count (pinning). It happens every time a page is requested but not released.
     */    
    public void pin() {
        this.pin_count++;
    }
    
    /**
     * Decrement pin_count (unpinning). It happens when the page is released.
     */        
    public void unpin() {
        this.pin_count--;
    }

    /**
     * Returns the number of empty slots on this page.
     */
    public int getNumEmptySlots() {
        return header.findSlots(false).size();
    }

    /**
     * Returns true if associated slot on this page is filled.
     */
    public boolean getSlot(int i) {
        return header.getSlotVal(i);
    }

    /**
     * Abstraction to fill a slot on this page.
     */
    public void setSlot(int i, boolean value) {
        header.setSlotVal(i, value);
    }

    /**
     * @return an iterator over all tuples on this page (calling remove on this
     *         iterator throws an UnsupportedOperationException) (note that this
     *         iterator shouldn't return tuples in empty slots!)
     */
    public Iterator<Tuple> iterator() {
        return new HeapPageIterator(this);
    }

    public int getNumSlots() {
        return this.numSlots;
    }

}