package simpledb;

import java.util.*;
import java.util.function.BiConsumer;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield; 
    private Type gbfieldtype;
    
    private TupleDesc td;
    private LinkedHashMap<Field,Integer> primaryMap;
    private Integer count;


    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) throws IllegalArgumentException {
        // some code goes here
        this.gbfield=gbfield;
        this.gbfieldtype=gbfieldtype;

        if(what.toString()!="count") {
            throw new IllegalArgumentException("must be a count aggregation");
        }

        if(gbfieldtype!=null) {
        	primaryMap=new LinkedHashMap<>();
            this.td=new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE}, new String[]{"gb field", "count"});
        }
        else {
            this.count=0;
            this.td=new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"count"});
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if(gbfieldtype==null) {
            count++;
        }
        else {
            Field tpField=tup.getField(gbfield);
            if(primaryMap.get(tpField)==null) {
                primaryMap.put(tpField, 1);
            }
            else {
                Integer actual=primaryMap.get(tpField);
                primaryMap.put(tpField, actual+1);
            }
            
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        List<Tuple> toRet=new ArrayList<>();
    	if(gbfieldtype==null) {
    		Tuple tup=new Tuple(this.td);
            tup.setField(0, new IntField(this.count));
    		toRet.add(tup);
    	}
    	else {    		
			BiConsumer<Field,Integer> myBiCons = (k,v) -> {
	    		Tuple tup=new Tuple(this.td);
	    		tup.setField(0, k);
	    		tup.setField(1, new IntField(v));
	    		toRet.add(tup);
			};
			primaryMap.forEach(myBiCons);
    		
    	}
    	
    	OpIterator opIter = new OpIterator() {
    		public TupleDesc tupleDescription=td;
    		public Iterator<Tuple> tupleIter;
    		
			@Override
			public void open() throws DbException, TransactionAbortedException {
				this.tupleIter=toRet.iterator();
				
			}

			@Override
			public boolean hasNext() throws DbException, TransactionAbortedException {
				return this.tupleIter.hasNext();
			}

			@Override
			public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
				return this.tupleIter.next();
			}

			@Override
			public void rewind() throws DbException, TransactionAbortedException {
				this.close();
				this.open();
			}

			@Override
			public TupleDesc getTupleDesc() {
				return this.tupleDescription;
			}

			@Override
			public void close() {
				this.tupleIter=null;
			}    		
    	};
    	
    	return opIter;
    }

}
