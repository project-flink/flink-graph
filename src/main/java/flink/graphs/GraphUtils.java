package flink.graphs;
import org.apache.avro.generic.GenericData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("serial")
public class GraphUtils {

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static DataSet<Integer> count(DataSet set) {
        List<Integer> nullValueList = new ArrayList<>();
        nullValueList.add(0);
        DataSet<Integer> nullValue = ExecutionEnvironment.getExecutionEnvironment().fromCollection(nullValueList);

        return set
                .map(new OneMapper())
                .union(nullValue)
                .reduce(new AddOnesReducer())
                .first(1);
    }

    private static final class OneMapper<T extends Tuple> implements MapFunction<T, Integer> {
        @Override
        public Integer map(T o) throws Exception {
            return 1;
        }
    }

    private static final class AddOnesReducer implements ReduceFunction<Integer> {
        @Override
        public Integer reduce(Integer one, Integer two) throws Exception {
            return one + two;
        }
    }

}