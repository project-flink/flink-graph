package flink.graphs.utils;

import java.io.Serializable;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import flink.graphs.Vertex;

/**
 * Map function that converts a {@link Vertex} object into a {@link Tuple2}.
 * @param <K> the key type for edge and vertex identifiers
 * @param <VV> the value type for vertexes
 */
public class VertexToTuple2Map<K extends Comparable<K> & Serializable,
	VV extends Serializable> implements MapFunction<Vertex<K, VV>, Tuple2<K, VV>> {

	private static final long serialVersionUID = 1L;

	public Tuple2<K, VV> map(Vertex<K, VV> vertex) {
		return new Tuple2<K, VV>(vertex.f0, vertex.f1);
	}

}
