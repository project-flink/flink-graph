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
import flink.graphs.Vertex;
import flink.graphs.example.utils.ExampleUtils;

public class ClusteringCoefficientExample implements ProgramDescription {
	
    @SuppressWarnings({ "serial" })
	public static void main (String [] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Vertex<Long, Double>> vertices = ExampleUtils.getLongDoubleVertexData(env);
        DataSet<Edge<Long, Double>> edges = ExampleUtils.getLongDoubleEdgeData(env);
        
        Graph<Long, Double, Double> graph = Graph.create(vertices, edges, env);

        DataSet<Tuple2<Long, HashSet<Long>>> neighborhoods = 
        		graph.reduceOnEdges(new EdgesFunction<Long, Double, Tuple2<Long, HashSet<Long>>>() {
        	
			@Override
			public Tuple2<Long, HashSet<Long>> iterateEdges(
					Iterable<Tuple2<Long, Edge<Long, Double>>> edges)
					throws Exception {
				
				Long vertexId = null;
				HashSet<Long> neighbors = new HashSet<Long>();
				
				for (Tuple2<Long, Edge<Long, Double>> edge : edges) {
					vertexId = edge.f0;
					neighbors.add(edge.f1.f1);
				}
				
				return new Tuple2<Long, HashSet<Long>>(vertexId, neighbors);
			}
		}, EdgeDirection.OUT);
        
        Graph<Long, HashSet<Long>, Double> newGraph = graph
        		.mapVertices(new MapFunction<Vertex<Long,Double>, HashSet<Long>>() {
					@Override
					public HashSet<Long> map(Vertex<Long, Double> arg)
							throws Exception {
						return new HashSet<Long>();
					}
				})
				.joinWithVertices(neighborhoods, new MapFunction<Tuple2<HashSet<Long>, HashSet<Long>>, HashSet<Long>>() {

					@Override
					public HashSet<Long> map(
							Tuple2<HashSet<Long>, HashSet<Long>> arg)
							throws Exception {
						
						return arg.f0;
					}
				});
        
        newGraph.getVertices().print();

        env.execute();
    }

    @Override
    public String getDescription() {
        return "Clustering Coefficient";
    }
}
