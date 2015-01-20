package flink.graphs.example.utils;

import flink.graphs.Edge;
import flink.graphs.Graph;
import flink.graphs.Vertex;
import flink.graphs.library.MinSpanningTree;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

public class MinSpanningTreeExample implements ProgramDescription {

    private static int maxIterations = 2;

    public static void main (String [] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Vertex<Long, String>> vertices = ExampleUtils.getLongStringVertexDataMST(env);

        DataSet<Edge<Long, Double>> edges = ExampleUtils.getLongDoubleEdgeDataMST(env);

        Graph<Long, String, Double> graph = Graph.fromDataSet(vertices, edges, env);

        DataSet<Edge<Long, Double>> minimumSpanningTree = graph.run(new MinSpanningTree(maxIterations)).getEdges();

        minimumSpanningTree.print();

        minimumSpanningTree.getExecutionEnvironment().execute();
    }

    @Override
    public String getDescription() {
        return "A Parallel Version of Boruvka's Minimum Spanning Tree Algorithm";
    }
}
