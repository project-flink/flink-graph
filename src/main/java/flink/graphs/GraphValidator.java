package flink.graphs;
import java.io.Serializable;

import org.apache.commons.lang.NotImplementedException;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

/**
 * A set of methods for validation of different types of Graphs
 *
 * @param <K>
 * @param <VV>
 * @param <EV>
 */
public class GraphValidator<K extends Comparable<K> & Serializable, VV extends Serializable,
	EV extends Serializable> {
	
	private final DataSet<Tuple2<K, VV>> vertices;
	private final DataSet<Tuple3<K, K, EV>> edges;
	
	public GraphValidator(DataSet<Tuple2<K, VV>> inputVertices, 
			DataSet<Tuple3<K, K, EV>> inputEdges) {
		this.vertices = inputVertices;
		this.edges = inputEdges;
	}

	/**
	 * Checks that the edge set input contains valid vertex Ids, 
	 * i.e. that they also exist in the vertex input set.
	 * @param vertices
	 * @param edges
	 * @throws InvalidVertexIdException
	 */
	 @SuppressWarnings("serial")
	public void validateGraph() throws InvalidVertexIdException {
		 DataSet<Tuple1<K>> edgeIds = edges.flatMap(new FlatMapFunction<Tuple3<K, K, EV>, 
				 Tuple1<K>>() {
			public void flatMap(Tuple3<K, K, EV> edge, Collector<Tuple1<K>> out) {
				out.collect(new Tuple1<K>(edge.f0));
				out.collect(new Tuple1<K>(edge.f1));
			}
		 }).distinct();
		 DataSet<K> invalidIds = vertices.coGroup(edgeIds).where(0).equalTo(0)
				 .with(new CoGroupFunction<Tuple2<K, VV>, Tuple1<K>, K>() {
					public void coGroup(Iterable<Tuple2<K, VV>> vertexId,
							Iterable<Tuple1<K>> edgeId, Collector<K> out) {
						if (!(vertexId.iterator().hasNext())) {
							// found an id that doesn't exist in the vertex set
							out.collect(edgeId.iterator().next().f0);
						}
					}
				}).first(1);
		 // check if invalidIds has any elements
	 }
	 
	 /**
	  * A strict graph is an unweighted, undirected graph 
	  * containing no graph loops or multiple edges.
	  */
	 public void validateStrictGraph() {
		 throw new NotImplementedException();
	 }
	 
	 /**
	  * A multi-graph is a strict graph that allows parallel edges.
	  */
	 public void validateMultiGraph() {
		 throw new NotImplementedException();
	 }

	 public void validateBipartiteGraph() {
		 throw new NotImplementedException();
	 }
}
