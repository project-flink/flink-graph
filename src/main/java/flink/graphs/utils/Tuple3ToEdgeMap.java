package flink.graphs.utils;

import flink.graphs.Edge;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

import java.io.Serializable;

/**
 * Map function that converts a {@link Tuple3} object into an {@link Edge}.
 * @param <K> the key type for edge and vertex identifiers
 * @param <EV> the value type for edges
 */
public class Tuple3ToEdgeMap<K extends Comparable<K> & Serializable,
	EV extends Serializable> implements MapFunction<Tuple3<K, K, EV>, Edge<K, EV>> {

	private static final long serialVersionUID = 1L;

	public Edge<K, EV> map(Tuple3<K, K, EV> tuple) {
		return new Edge<>(tuple.f0, tuple.f1, tuple.f2);
	}

}
