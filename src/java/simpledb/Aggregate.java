package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private final int groupingField;
    private final int aggregateField;
    private final Aggregator.Op aggregationOperator;
    private OpIterator opChild;
    private OpIterator iterator;
    private Aggregator aggregator;

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The OpIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        // some code goes here
        this.groupingField = gfield;
        this.aggregateField = afield;
        this.aggregationOperator = aop;
        this.opChild = child;
        TupleDesc tupleDesc = opChild.getTupleDesc();
        Type aggregatorType = tupleDesc.getFieldType(afield);
        Type gFieldType = aggregatorType;


    if (aggregatorType == Type.INT_TYPE){
        // Description of gField
        // The column over which we are grouping the result, or -1 if there is no grouping
        if (gfield == -1){
            gFieldType = null;
        }
        else {
            gFieldType = tupleDesc.getFieldType(gfield);
        }
        aggregator = new IntegerAggregator(gfield, gFieldType, afield, aop);
    }
    else if (aggregatorType == Type.STRING_TYPE){
        if (gfield == -1){
            gFieldType = null;
        }
        else {
            gFieldType = tupleDesc.getFieldType(gfield);
        }
        aggregator = new StringAggregator(gfield, gFieldType, afield, aop);
    }
    iterator = aggregator.iterator();
}

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	// some code goes here
	    return groupingField;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() {
	// some code goes here
        if (groupingField == Aggregator.NO_GROUPING){
            return null;
        }
        else{
            return opChild.getTupleDesc().getFieldName(groupingField);
        }
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	// some code goes here
	    return aggregateField;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	// some code goes here
	    return opChild.getTupleDesc().getFieldName(aggregateField);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	// some code goes here
	    return aggregationOperator;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	    return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
	    // some code goes here
        opChild.open();
        super.open();
        while (opChild.hasNext()){
            aggregator.mergeTupleIntoGroup(opChild.next());
        }
        iterator = aggregator.iterator();
        iterator.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	    // some code goes here
        if (iterator.hasNext()){
            return iterator.next();
        }
	    return null;
    }


    public void rewind() throws DbException, TransactionAbortedException {
	    // some code goes here
        iterator.rewind();
        opChild.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	    // some code goes here
        Type[] fieldType;
        String[] fieldName;
        String aggregatorName = opChild.getTupleDesc().getFieldName(aggregateField);
        
        if (aggregateField == Aggregator.NO_GROUPING){
            fieldName = new String[1];
            fieldType = new Type[1];
            fieldType[0] = opChild.getTupleDesc().getFieldType(aggregateField);

            if (aggregatorName != null){
                fieldName[0] = nameOfAggregatorOp(aggregationOperator) + "(" + opChild.getTupleDesc().getFieldName(aggregateField); // not complete
            }
        }

        else {
            fieldName = new String[2];
            fieldType = new Type[2];

            fieldType[0] = opChild.getTupleDesc().getFieldType(groupingField);
            fieldName[0] = opChild.getTupleDesc().getFieldName(groupingField);
            fieldType[1] = opChild.getTupleDesc().getFieldType(aggregateField);

            if (aggregatorName != null) {
                fieldName[1] = nameOfAggregatorOp(aggregationOperator) + "(" + opChild.getTupleDesc().getFieldName(aggregateField); // not complete
                }
            }
        
        return new TupleDesc(fieldType, fieldName);
        }
        

    public void close() {
	// some code goes here
        super.close();
        iterator.close();
        opChild.close();
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[] {
            opChild
        };
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        if (this.opChild != children[0]){
            opChild = children[0];
        }
    }
    
}
