package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */

public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    int gbIndex;
    int agIndex;
    TupleDesc originalTd;
    TupleDesc td;
    Type gbFieldType;
    Op aggreOp;
    HashMap<Field, Integer> gval2agval;
    HashMap<Field, Integer[]> gval2count_sum;


    public IntegerAggregator(int gbIndex, Type gbFieldType, int agIndex, Op aggreOp,TupleDesc td) {
        // some code goes here
        this.gbIndex = gbIndex;
        this.gbFieldType = gbFieldType;
        this.agIndex = agIndex;
        this.aggreOp = aggreOp;
        this.td=td;
        gval2agval = new HashMap<>();
        gval2count_sum = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     * @throws IllegalArgumentException
     */
    public void mergeTupleIntoGroup(Tuple tup) throws IllegalArgumentException {
        // some code goes here
        Field aggreField;
        Field gbField = null;
        Integer newVal;
        aggreField = tup.getField(agIndex);
        int toAggregate;
        if (aggreField.getType() != Type.INT_TYPE) {
            throw new IllegalArgumentException("该tuple的指定列不是Type.INT_TYPE类型");
        }
        toAggregate = ((IntField) aggreField).getValue();
        if (originalTd == null) {
            originalTd = tup.getTupleDesc();
        } else if (!originalTd.equals(tup.getTupleDesc())) {
            throw new IllegalArgumentException("待聚合tuple的tupleDesc不一致");
        }
        if (gbIndex != Aggregator.NO_GROUPING) {
            gbField = tup.getField(gbIndex);
        }
        if (aggreOp == Op.AVG) {
            if (gval2count_sum.containsKey(gbField)) {
                Integer[] oldCountAndSum = gval2count_sum.get(gbField);
                int oldCount = oldCountAndSum[0];
                int oldSum = oldCountAndSum[1];
                gval2count_sum.put(gbField, new Integer[]{oldCount + 1, oldSum + toAggregate});
            } else {
                gval2count_sum.put(gbField, new Integer[]{1, toAggregate});
            }
            Integer[] c2s=gval2count_sum.get(gbField);
            int currentCount = c2s[0];
            int currentSum = c2s[1];
            gval2agval.put(gbField, currentSum / currentCount);

            return;
        }

        if (gval2agval.containsKey(gbField)) {
            Integer oldVal = gval2agval.get(gbField);
            newVal = calcuNewValue(oldVal, toAggregate, aggreOp);
        } else if (aggreOp == Op.COUNT) {
            newVal = 1;
        } else {
            newVal = toAggregate;
        }
        gval2agval.put(gbField, newVal);
    }

   
    private int calcuNewValue(int oldVal, int toAggregate, Op aggreOp) {
        switch (aggreOp) {
            case COUNT:
                return oldVal + 1;
            case MAX:
                return Math.max(oldVal, toAggregate);
            case MIN:
                return Math.min(oldVal, toAggregate);
            case SUM:
                return oldVal + toAggregate;
            default:
                throw new IllegalArgumentException("不应该到达这里");
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        ArrayList<Tuple> tuples = new ArrayList<>();
        for (Map.Entry<Field, Integer> g2a : gval2agval.entrySet()) {
            Tuple t = new Tuple(td);

            if (gbIndex == Aggregator.NO_GROUPING) {
                t.setField(0, new IntField(g2a.getValue()));
            } else {
                t.setField(0, g2a.getKey());
                t.setField(1, new IntField(g2a.getValue()));
            }
            tuples.add(t);
        }
        return new TupleIterator(td, tuples);
    }

}