package simpledb;

import java.util.*;

public class HeapPageIterator implements Iterator<Tuple> {
    private HeapPage _page;
    private int _numTuples;
    private int _currentTuple;
        
    // Assumes pages cannot be modified while iterating over them
    // Iterates over only valid tuples
    public HeapPageIterator(HeapPage page) {
        _page = page;
        _currentTuple = 0;
        _numTuples = _page.getNumSlots()-_page.getNumEmptySlots();
	//			if(_numTuples>1)System.out.println(_page.getNumSlots()+":"+_page.getNumEmptySlots()+":"+_numTuples);
    }
        
    public boolean hasNext() {
	//System.out.println(_currentTuple+":"+_numTuples);
        return _currentTuple < _numTuples;
    }

    public void rewind() {
        _currentTuple=0;
    }
        
    public Tuple next() {
	//System.out.print("; T"+_currentTuple);
        return _page.tuples[_currentTuple++];
    }

    public boolean hasPrevious() {
	//System.out.println(_currentTuple+":"+_numTuples);
        return _currentTuple > 0;
    }

    public void setLast() {
	//System.out.println(_currentTuple+":"+_numTuples);
        _currentTuple = _numTuples;
    }

         
    public Tuple previous() {
	//System.out.print("; T"+_currentTuple);
        return _page.tuples[--_currentTuple];
    }
        
    public void remove() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("Cannot remove on HeapPageIterator");
    }
}