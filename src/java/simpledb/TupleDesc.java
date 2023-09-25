package simpledb;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

	public List<TDItem> fields = new ArrayList<>();
    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        public final Type fieldType; 
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
        
        public String getFieldName() {
        	return fieldName;
        }
        
        public Type getFieldType() {
        	return fieldType;
        }
    }
    
    
    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        return getFields().iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        if(typeAr.length==fieldAr.length) {
        	for (int i = 0; i< typeAr.length; i++) {
        		fields.add(new TDItem(typeAr[i],fieldAr[i]));
        	}
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
    	for (int i = 0; i< typeAr.length; i++) {
    		fields.add(new TDItem(typeAr[i], null));
    	}
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return getFields().size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if (i>=this.numFields() || i<0) {
        	throw new NoSuchElementException();
        }
        return getFields().get(i).getFieldName();
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
    	if (i>=this.numFields() || i<0) {
        	throw new NoSuchElementException();
        }
        return getFields().get(i).getFieldType();
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
    	int i = this.getFields().stream().map(TDItem::getFieldName).collect(Collectors.toList()).indexOf(name);
    	if (i !=-1) {
    		return i;
    	}
    	else {
    		throw new NoSuchElementException();
    	}
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        //return this.getFields().stream().map(TDItem::getFieldType).mapToInt(Type::getLen).sum();
    	int size=0;
    	for (int i = 0; i<fields.size(); i++) {
    		size += fields.get(i).getFieldType().getLen();
    	}
    	return size;
    
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
    	List<TDItem> mergedItems = new ArrayList<TDItem>(td1.getFields());
        mergedItems.addAll(td2.getFields());
        Type[] t = mergedItems.stream().map(TDItem::getFieldType).toArray(Type[]::new);
        String[] s = mergedItems.stream().map(TDItem::getFieldName).toArray(String[]::new);
       
        return new TupleDesc(t,s);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
    	if (o instanceof TupleDesc) {
    		TupleDesc td = (TupleDesc) o;
            if(td.numFields()==this.numFields()) {
    	        for (int i=0; i<td.numFields(); i++) {
    	        	if(td.getFieldType(i)!=this.getFieldType(i)) {
    	        		return false;
    	        	}
    	        };
    	        return true;
            }
    	}
        return false;
    }

    public int hashCode() {
    	String s ="";
    	for (int i=0; i<this.numFields(); i++) {
    		s+=this.getFieldType(i).toString();
        };
        return s.hashCode();
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
    	String s="";
    	for (int i=0; i<this.numFields(); i++) {
    		s+=this.getFieldName(i);
    		s+="[";
    		s+=i;
    		s+="](";
    		s+=this.getFieldType(i).toString();
    		s+="),";
        };
        return s;
    }

	public List<TDItem> getFields() {
		return fields;
	}
}
