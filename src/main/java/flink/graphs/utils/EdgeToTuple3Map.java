package flink.graphs.utils;

import java.io.Serializable;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

import flink.graphs.Edge;

/**
 * Map function that converts an {@link Edge} object into a {@link Tuple3}.
 * @param <K> the key type for edge and vertex identifiers
 * @param <EV> the value type for edges
 */
public class EdgeToTuple3Map<K extends Comparable<K> & Serializable, 
	EV extends Serializable> implements MapFunction<Edge<K, EV>, Tuple3<K, K, EV>> {

	private static final long serialVersionUID = 1L;

	public Tuple3<K, K, EV> map(Edge<K, EV> edge) {
		return new Tuple3<K, K, EV>(edge.f0, edge.f1, edge.f2);
	}

}
