package simpledb;
import java.util.*;
import java.io.*;

//added
import simpledb.Predicate.Op;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends AbstractDbIterator {

    private JoinPredicate _predicate;
    private DbIterator _outerRelation;
    private DbIterator _innerRelation;
    private Iterator<Tuple> _outerPage=null;
    private Iterator<Tuple> _innerPage=null;

    private Tuple _outerRecent=null;
    private Tuple _innerRecent=null;

    private int _joinType = 1;
    private int _numMatches =0;
    private int _numComp=0;

    //added
    private TupleDesc _td1;
    private TupleDesc _td2;
    private Tuple _firstMatch = null; //First match for outer tuple in inner relation's partition.
    //int pointerToOuterTuple = 0;
    //int pointerToInnerTuple;
  
    public static final int SNL = 0;
    public static final int PNL = 1;    
    public static final int BNL = 2;    
    public static final int SMJ = 3;    
    public static final int HJ = 4;    
    /**
     * Constructor.  Accepts to children to join and the predicate
     * to join them on
     *
     * @param p The predicate to use to join the children
     * @param child1 Iterator for the left(outer) relation to join
     * @param child2 Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, DbIterator child1, DbIterator child2) {
	//IMPLEMENT THIS
        //added
        this._predicate = p;
        this._outerRelation = child1;
        this._innerRelation = child2;

        this._td1 = child1.getTupleDesc();
        this._td2 = child1.getTupleDesc();
    }

    public void setJoinAlgorithm(int joinAlgo){
	_joinType = joinAlgo;
    }
    /**
     * @see simpledb.TupleDesc#combine(TupleDesc, TupleDesc) for possible implementation logic.
     */
    public TupleDesc getTupleDesc() {
	//IMPLEMENT THIS
        //added

	   return (TupleDesc.combine(this._td1,this._td2));
    }

    public void open()
        throws DbException, NoSuchElementException, TransactionAbortedException, IOException {
		//IMPLEMENT THIS
            //added
            _outerRelation.open();
            _innerRelation.open();

            _outerRecent = _outerRelation.next();
            _innerRecent = _innerRelation.next();

    }

    public void close() {

//IMPLEMENT THIS
        //added
        _innerRelation.close();
        _outerRelation.close();
        
    }

    public void rewind() throws DbException, TransactionAbortedException, IOException {
//IMPLEMENT THIS
        //added
        _innerRelation.rewind();
        _outerRelation.rewind();

    }

    /**
     * Returns the next tuple generated by the join, or null if there are no more tuples.
     * Logically, this is the next tuple in r1 cross r2 that satisfies the join
     * predicate.  There are many possible implementations; the simplest is a
     * nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of
     * Join are simply the concatenation of joining tuples from the left and
     * right relation. Therefore, there will be two copies of the join attribute
     * in the results.  (Removing such duplicate columns can be done with an
     * additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     *
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple readNext() throws TransactionAbortedException, DbException {
	switch(_joinType){
	case SNL: return SNL_readNext();
	case PNL: return PNL_readNext();
	case BNL: return BNL_readNext();
	case SMJ: return SMJ_readNext();
	case HJ: return HJ_readNext();
	default: return SNL_readNext();
	}
    }

    protected Tuple SNL_readNext() throws TransactionAbortedException, DbException {
	//IMPLEMENT THIS 
        //added
        try{ //to catch unhandled exceptions

            while(_outerRecent!=null){ //iterate over all tuples of outer
                while(_innerRecent!=null){ //iterate over all tuples if inner
                    Tuple _joinedTuple = null; //the output joined tuple
                    if(_predicate.filter(_outerRecent,_innerRecent)){ //if the attributes match
                        _joinedTuple = joinTuple(_outerRecent,_innerRecent,getTupleDesc()); //join the tuple usimg the method described below
                        _numMatches = _numMatches + 1; //count the number of matching tuples 
                    }

                    if(_innerRelation.hasNext()){ //if the inner relation still has tuples remaining
                        _innerRecent= _innerRelation.next(); //go to the next tuple
                    }
                    else{   //if the inner relation doesnt have any tuples remaining
                        _innerRecent = null; //set the next tuple as null so that it breaks out of the inner while loop
                    }   
                    
                    if(_joinedTuple!=null){ //if there was a match for this inner tuple and the current outer tuple
                        return _joinedTuple; //return the join of the matching tuples
                    }
                }
                if(_outerRelation.hasNext()){ //if the outer relation still has tuples remaining
                    _outerRecent = _outerRelation.next(); //go to the next tuple
                    _innerRelation.rewind(); //re-iterate over the entire inner relation again
                    _innerRecent = _innerRelation.next(); //assign the first tuple of inner as the current inner tuple for the next loop
                }
                else{ //if the outer relation doesnt have any tuples remaining
                    _outerRecent = null; //set the next tuple of outer as null so that it breaks out of the outer while loop
                }
            }

        }

        catch(IOException e){}; //IOExeption not reported in SNL_readNext()

	return null; //return null if there are no joining tuples
    }


    protected Tuple PNL_readNext() throws TransactionAbortedException, DbException {
	//IMPLEMENT THIS (EXTRA CREDIT ONLY)
       return null;
    }


    protected Tuple BNL_readNext() throws TransactionAbortedException, DbException {
	//no need to implement this
	return null;
    }


    protected Tuple SMJ_readNext() throws TransactionAbortedException, DbException {
	
	//IMPLEMENT THIS. YOU CAN ASSUME THE JOIN PREDICATE IS ALWAYS =
        //Assuming the tuples are in sorted order!!
        
       Tuple resultTuple = null;
        
        try {
            while((_outerRelation.hasNext() && _innerRelation.hasNext()) || (_outerRecent != null)) {
                if (_outerRecent == null) {
                    _outerRecent = _outerRelation.next();
                }
                
                if (_innerRecent == null) {
                    if (_innerRelation.hasNext()) {
                        _innerRecent = _innerRelation.next();
                    } else {
                        // If we are at end of inner relation, then increment the outer relation and reset the inner relation's iterator back to first match.
                        if (_outerRelation.hasNext()) {
                            _outerRecent = _outerRelation.next();
                            if (_firstMatch != null && _predicate.filter(_outerRecent, _firstMatch)) {
                                _innerRecent = _innerRelation.seek(_firstMatch.getRecordID());
                            }
                            
                            _firstMatch = null;
                        }
                    }
                    
                    if (_innerRecent == null) {
                        break;
                    }
                }
                
                // If predicate matches then join the tuples and proceed to next tuple in inner relation
                if (_predicate.filter(_outerRecent, _innerRecent)) {
                    ++_numMatches;
                    ++_numComp;

                    resultTuple = joinTuple(_outerRecent, _innerRecent, getTupleDesc());

                    // If this is the first tuple in partition then store its value to later jump to this position using its record id.
                    if (_firstMatch == null) {
                        _firstMatch = _innerRecent;
                    }

                    _innerRecent = null;
                    
                    break;
                } else if (_predicate.getLeftField(_outerRecent).compare(Op.LESS_THAN, _predicate.getRightField(_innerRecent))) {
                    ++_numComp;

                    if (_outerRelation.hasNext()) {
                        _outerRecent = _outerRelation.next();
                        
                        /* If the tuples in the outer relation have duplicates then we need to join them too with the
                            previous matches in the inner relation, so we take the inner relation's iterator back to the first match's position.
                        */
                        if (_firstMatch != null && _predicate.filter(_outerRecent, _firstMatch)) {
                            _innerRecent = _innerRelation.seek(_firstMatch.getRecordID());
                        }
                        
                        _firstMatch = null;
                    } else {
                        _outerRecent = null;
                    }
                } else {
                    ++_numComp;
                    _innerRecent = null;
                    
                    if (_firstMatch != null) {
                        _firstMatch = null;
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        return resultTuple;
    }  
            
          
    //return null;
    //}


    protected Tuple HJ_readNext() throws TransactionAbortedException, DbException {
	//no need to implement this
	return null;
    }


    private Tuple joinTuple(Tuple outer, Tuple inner, TupleDesc tupledesc){
	//IMPLEMENT THIS
        //added
        Tuple resultingTuple = new Tuple(tupledesc); //create the result tuple with the given tuple desscription
        int fieldCount = 0; //variable to iterate over all the attributes outer and inner relation

        for(int i=0; i<outer.getTupleDesc().numFields(); i++){ //iterate over all the fields of outer tuple
            resultingTuple.setField(fieldCount, outer.getField(i)); //set each field of the result tuple
            fieldCount++; //increment the field count
        }

        for(int i=0; i<inner.getTupleDesc().numFields(); i++){ //iterate over all the fields of inner tuple
            resultingTuple.setField(fieldCount, inner.getField(i)); //set each field of the result tuple
            fieldCount++; //increment the field count
        }

	return resultingTuple;
    }

    public int getNumMatches(){
	return _numMatches;
    }
    public int getNumComp(){
	return _numComp;
    }
}
