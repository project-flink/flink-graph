package flink.graphs.example;

import java.util.HashSet;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

import flink.graphs.Edge;
import flink.graphs.EdgeDirection;
import flink.graphs.EdgesFunction;
import flink.graphs.Graph;
import flink.graphs.NeighborsFunctionWithVertexValue;
import flink.graphs.Vertex;
import flink.graphs.example.utils.ExampleUtils;

public class ClusteringCoefficientExample implements ProgramDescription {
	
    @SuppressWarnings({ "serial" })
	public static void main (String [] args) throws Exception {
    	
          ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Vertex<Long, Double>> vertices = ExampleUtils.getLongDoubleVertexData(env);
        DataSet<Edge<Long, Double>> edges = ExampleUtils.getLongDoubleEdgeData(env);
        
        Graph<Long, Double, Double> graph = Graph.create(vertices, edges, env);

        // Get the neighbors of each vertex in a HashSet
        DataSet<Tuple2<Long, HashSet<Long>>> neighborhoods = 
        		graph.reduceOnEdges(new EdgesFunction<Long, Double, Tuple2<Long, HashSet<Long>>>() {
        	
			@Override
			public Tuple2<Long, HashSet<Long>> iterateEdges(
					Iterable<Tuple2<Long, Edge<Long, Double>>> edges) throws Exception {
				
				Long vertexId = null;
				HashSet<Long> neighbors = new HashSet<Long>();
				
				for (Tuple2<Long, Edge<Long, Double>> edge : edges) {
					vertexId = edge.f0;
					neighbors.add(edge.f1.f1);
				}
				
				return new Tuple2<Long, HashSet<Long>>(vertexId, neighbors);
			}
		}, EdgeDirection.OUT);
        
        // Construct a new graph where the neighborhood is the vertex value
        Graph<Long, HashSet<Long>, Double> newGraph = graph
        		.mapVertices(new MapFunction<Vertex<Long,Double>, HashSet<Long>>() {
					@Override
					public HashSet<Long> map(Vertex<Long, Double> arg) throws Exception {
						return new HashSet<Long>();
					}
				})
				.joinWithVertices(neighborhoods, new MapFunction<Tuple2<HashSet<Long>, HashSet<Long>>, HashSet<Long>>() {
					@Override
					public HashSet<Long> map(Tuple2<HashSet<Long>, HashSet<Long>> arg) throws Exception {
						return arg.f1;
					}
				});
        
        // Calculate clustering coefficient
        newGraph.reduceOnNeighbors(new NeighborsFunctionWithVertexValue<Long, HashSet<Long>, Double, Tuple2<Long, Double>>() {

			@Override
			public Tuple2<Long, Double> iterateNeighbors(
					Vertex<Long, HashSet<Long>> vertex,
					Iterable<Tuple2<Edge<Long, Double>, Vertex<Long, HashSet<Long>>>> neighbors) throws Exception {
				
				int deg = vertex.f1.size();
				int e = 0;
				
				// Calculate common neighbor count (e)
				for (Tuple2<Edge<Long, Double>, Vertex<Long, HashSet<Long>>> neighbor : neighbors) {
					
					// Iterate neighbor's neighbors
					for (Long nn : neighbor.f1.f1) {
						if (vertex.f1.contains(nn)) {
							e++;
						}
					}
				}
				
				// Calculate clustering coefficient
				double cc;
				
				if (deg > 1) {
					cc = (double) e / (double) (deg * (deg - 1));
				} else {
					cc = 0.0;
				}
				
				return new Tuple2<Long, Double>(vertex.f0, cc);
			}
        	
		}, EdgeDirection.OUT).print();

        env.execute();
    }

    @Override
    public String getDescription() {
        return "Clustering Coefficient";
    }
}
