package flink.graphs.utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import flink.graphs.Edge;
import flink.graphs.Vertex;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.NullValue;

@SuppressWarnings("serial")
public class GraphUtils {

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static DataSet<Integer> count(DataSet set, ExecutionEnvironment env) {
		List<Integer> list = new ArrayList<>();
		list.add(0);
		DataSet<Integer> initialCount = env.fromCollection(list);
        return set
                .map(new OneMapper())
                .union(initialCount)
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

	/**
	 * Reads edges with value from CSV file
	 */
	@SuppressWarnings({ "unchecked" })
	public static <K extends Comparable<K> & Serializable, EV extends Serializable>
		DataSet<Edge<K, EV>> readEdgesFromCsvFile(ExecutionEnvironment env,
												  String filePath,
												  char delimiter,
												  Class<K> VertexIdClass,
												  Class<EV> EdgeValueClass) {

		CsvReader reader = new CsvReader(filePath, env);
		return (DataSet<Edge<K, EV>>) (DataSet<?>) reader.fieldDelimiter(delimiter)
				.types(VertexIdClass, VertexIdClass, EdgeValueClass);
	}

	/**
	 * Reads edges from CSV file that have no value
	 */
	public static <K extends Comparable<K> & Serializable>
		DataSet<Edge<K, NullValue>> readEdgesFromCsvFile(ExecutionEnvironment env,
														 String filePath,
														 char delimiter,
														 Class<K> VertexIdClass) {

		CsvReader reader = new CsvReader(filePath, env);
		return reader.fieldDelimiter(delimiter).types(VertexIdClass, VertexIdClass).map(
				new MapFunction<Tuple2<K, K>, Edge<K, NullValue>>() {
					public Edge<K, NullValue> map(Tuple2<K, K> tuple) throws Exception {
						return new Edge<K, NullValue>(tuple.f0, tuple.f1, NullValue.getInstance());
					}
				}
		);
	}

	/**
	 * Reads vertices from CSV file
	 */
	@SuppressWarnings({ "unchecked" })
	public static <K extends Comparable<K> & Serializable, VV extends Serializable>
		DataSet<Vertex<K, VV>> readVerticesFromCsvFile(ExecutionEnvironment env,
													   String filePath, char delimiter,
													   Class<K> VertexIdClass,
													   Class<VV> VertexValueClass) {

		CsvReader reader = new CsvReader(filePath, env);
		return (DataSet<Vertex<K, VV>>) (DataSet<?>) reader.fieldDelimiter(delimiter).types(VertexIdClass, VertexValueClass);
	}
}
