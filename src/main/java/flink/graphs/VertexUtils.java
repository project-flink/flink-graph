package flink.graphs;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class VertexUtils {

    /**
     * Counts the number of edges that respect the given filter function
     * @param environment
     * @param graph the initial graph
     * @param f the function used for filtering
     * @return  a counter representing the edges that respect f
     */
    public static final <K extends Comparable<K> & Serializable, V extends Serializable, EV extends Serializable> DataSet<Integer> countWhere
                                (ExecutionEnvironment environment, Graph<K, V, EV> graph, FilterFunction<Tuple3<K, K, EV>> f) {
        List<Integer> list = new ArrayList<>();
        list.add(0);
        DataSet<Integer> initialCount = environment.fromCollection(list);

        return graph.getEdges().filter(f).map(new MapFunction<Tuple3<K, K, EV>, Integer>() {
            public Integer map(Tuple3<K, K, EV> edge){
                return 1;
            }
        }).union(initialCount).reduce(new ReduceFunction<Integer>() {
            public Integer reduce(Integer val1, Integer val2) {
                return val1 + val2;
            }
        });
    }
}
