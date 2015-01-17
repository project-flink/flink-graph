package flink.graphs.example;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import flink.graphs.Edge;
import flink.graphs.Graph;
import flink.graphs.Vertex;
import flink.graphs.example.utils.ExampleUtils;
import flink.graphs.library.LocalClusteringCoefficient;

public class LocalClusteringCoefficientExample implements ProgramDescription {
	
	public static void main (String [] args) throws Exception {
    	
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Vertex<Long, Double>> vertices = ExampleUtils.getLongDoubleVertexData(env);
        DataSet<Edge<Long, Double>> edges = ExampleUtils.getLongDoubleEdgeData(env);
        
        Graph<Long, Double, Double> graph = Graph.create(vertices, edges, env);
        
        DataSet<Vertex<Long, Double>> clusteringCoefficients = 
        		graph.run(new LocalClusteringCoefficient<Long>()).getVertices();
        
        clusteringCoefficients.print();

        env.execute();
    }

    @Override
    public String getDescription() {
        return "Clustering Coefficient";
    }
}
