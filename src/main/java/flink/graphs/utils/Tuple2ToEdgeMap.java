package flink.graphs.utils;

import flink.graphs.Edge;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.NullValue;

import java.io.Serializable;

/**
 * Map function that converts a {@link Tuple2} object into an {@link Edge}.
 * @param <K> the key type for edge and vertex identifiers
 */
public class Tuple2ToEdgeMap<K extends Comparable<K> & Serializable>
		implements MapFunction<Tuple2<K, K>, Edge<K, NullValue>> {

	private static final long serialVersionUID = 1L;

	public Edge<K, NullValue> map(Tuple2<K, K> tuple) {
		return new Edge<>(tuple.f0, tuple.f1);
	}

}
