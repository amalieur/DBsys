package simpledb;

import java.util.*;
import java.util.function.BiConsumer;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    
    private LinkedHashMap<Field,Integer> primaryMap;
    private LinkedHashMap<Field,Integer> secondaryMap;
    private LinkedHashMap<Field,Integer> avgMap;
    private Integer aggregatedValue;
    private Integer count;
    
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield=gbfield;
        this.gbfieldtype=gbfieldtype;
        this.afield=afield;
        this.what=what;
        if(gbfieldtype!=null) {
        	primaryMap=new LinkedHashMap<>();
        	if (what.toString()=="avg") {
        		secondaryMap=new LinkedHashMap<>();
        		avgMap=new LinkedHashMap<>();
        	}
        }
		this.aggregatedValue=0;
        this.count=0;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
    	Integer tpValue=0;
    	if(what.toString()!="count") {
    		tpValue = ((IntField) tup.getField(afield)).getValue();
    	}
    	
		
        switch (what) {
        case MIN:
        	if(gbfieldtype==null) {
        		Integer actual=aggregatedValue;
            	if( actual==null || actual> tpValue) aggregatedValue=tpValue;
            }
        	else {
        		Field tpField=tup.getField(gbfield);
        		Integer actual=primaryMap.get(tpField);
        		if(actual==null || actual> tpValue) primaryMap.put(tpField, tpValue);
        	}
        	break;
        	
        case MAX:
        	if(gbfieldtype==null) {
        		Integer actual=aggregatedValue;
            	if(actual==null || actual< tpValue) aggregatedValue=tpValue;
            }
        	else {
        		Field tpField=tup.getField(gbfield);
        		Integer actual=primaryMap.get(tpField);
        		if(actual==null || actual< tpValue) primaryMap.put(tpField, tpValue);
        	}
        	break;
        	
        case SUM:
        	if(gbfieldtype==null) {
        		Integer actual=aggregatedValue;
            	if(actual==null) aggregatedValue=0;
            	aggregatedValue+=tpValue;
            }
        	else {
        		Field tpField=tup.getField(gbfield);
        		Integer actual=primaryMap.get(tpField);
        		if(actual!=null) primaryMap.put(tpField, actual+tpValue);
        		else primaryMap.put(tpField, tpValue);
        	}
        	break;
        case AVG:
	        if(gbfieldtype==null) {
	        	if(aggregatedValue==null) {
	        		aggregatedValue=0;
	        		count=0;
	        	}
	        	aggregatedValue+=tpValue;
	        	count++;
	        }
	    	else {
	    		Field tpField=tup.getField(gbfield);
				System.out.print("GB Field: ");
				System.out.println(tpField);
				System.out.print("Value: ");
				System.out.println(tpValue);
				
	    		if(primaryMap.containsKey(tpField)) {
	    			Integer actual=primaryMap.get(tpField);
					System.out.print("Actual: ");
					System.out.println(actual);
	    			primaryMap.put(tpField, actual+tpValue);
	    			secondaryMap.put(tpField, secondaryMap.get(tpField)+1);
	    		}
	    		else {
	    			primaryMap.put(tpField, tpValue);
	    			secondaryMap.put(tpField, 1);
	    		}
	    	}
	    	break;
        case COUNT:
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
	    	break;
        
		default:
			break;
		}
        
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     *         
     *         TupleDesc(Type[] typeAr, String[] fieldAr)
     */
    public OpIterator iterator() {
    	List<Tuple> toRet=new ArrayList<>();
    	TupleDesc td;
    	if(gbfieldtype==null) {
    		td= new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{what.toString()});
    		Tuple tup=new Tuple(td);
    		if(what.toString()=="avg") {
				if(count!=0) {
					tup.setField(0, new IntField((int) aggregatedValue/count));
				}
				else{
					tup.setField(0, new IntField(0));
				}
        	}
    		else {
    			tup.setField(0, new IntField(aggregatedValue));
    		}
    		toRet.add(tup);
    	}
    	else {
			td= new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE}, new String[]{"group by field", what.toString()});
			BiConsumer<Field,Integer> myBiCons = (k,v) -> {
				Tuple tup=new Tuple(td);
				tup.setField(0, k);
				tup.setField(1, new IntField(v));
				toRet.add(tup);
			};

    		if(what.toString()=="avg") {
    			BiConsumer<Field,Integer> avgCalc = (k,v) -> {
    	    		Integer sum=primaryMap.get(k);
					Integer avg = ((int) sum)/((int)v);
    	    		avgMap.put(k, avg);
    			};
        		secondaryMap.forEach(avgCalc);
				avgMap.forEach(myBiCons);
        	}
    		
			else {
				primaryMap.forEach(myBiCons);
			}
    		
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
