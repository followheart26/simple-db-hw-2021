package simpledb.storage;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    //表定义
    private TupleDesc td;

    //在磁盘中的记录位置
    private RecordId rid;

    //字段
    private CopyOnWriteArrayList<Field> fields;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        this.td = td;
        fields = new CopyOnWriteArrayList<>();
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        return this.rid;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        this.rid = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
        if(i >= 0 && i < fields.size()){
            fields.set(i,f);
        } else if (i == fields.size()) {
            fields.add(f);
        }
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        if(fields == null || i >= fields.size() || i<0){
            return null;
        }
        return fields.get(i);
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        //每一行输出列名+值，而列名在td里面，值在field里面
        StringBuilder sb = new StringBuilder();
        Iterator<TupleDesc.TDItem> tdItems = this.td.iterator();
        int i=0;
        while(tdItems.hasNext()){
            TupleDesc.TDItem item = tdItems.next();
            sb.append("FieldName: ").append(item.fieldName)
                    .append("==> Value: ").append(fields.get(i).toString())
                    .append(System.lineSeparator());
        }
        return sb.toString();
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        //list类本身就实现了iterable接口
        return fields.iterator();
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        this.td = td;
    }

    /**
     * 重写equals方法，如何判断两个元组是否相同应当判断其物理实体recordID是否相同
     * 还有表定义是否相同，
     * 最后是字段是否相同
     * @param obj
     * @return
     */
    @Override
    public boolean equals(Object obj) {

        if (!(obj instanceof  Tuple))
        {
            return false;
        }

        Tuple other = (Tuple) obj;
        if (this.rid.equals(other.getRecordId()) &&
                this.td.equals(other.getTupleDesc())) {
            for (int i = 0; i < this.fields.size(); i++) {
                if (!this.fields.get(i).equals(other.getField(i))) {
                    return false;
                }
            }
            return true;
        }

        return false;
    }
}
