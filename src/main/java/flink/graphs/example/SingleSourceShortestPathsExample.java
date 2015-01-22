package flink.graphs.example;

import flink.graphs.Edge;
import flink.graphs.Graph;
import flink.graphs.Vertex;
import flink.graphs.example.utils.ExampleUtils;
import flink.graphs.library.SingleSourceShortestPaths;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

public class SingleSourceShortestPathsExample implements ProgramDescription {

    private static int maxIterations = 5;
    private static String outputPath = null;
    private static boolean fileOutput = false;

    public static void main (String ... args) throws Exception {

        if(args.length > 0) {
            outputPath = args[0];
            maxIterations = Integer.parseInt(args[1]);
            fileOutput = true;
        } else {
            System.err.println("Usage: SingleSourceShortestPathsExample <resultPath> <max number of iterations>");
            fileOutput = false;
            return;
        }

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Vertex<Long, Double>> vertices = ExampleUtils.getLongDoubleVertexData(env);

        DataSet<Edge<Long, Double>> edges = ExampleUtils.getLongDoubleEdgeData(env);

        Long srcVertexId = 1L;

        Graph<Long, Double, Double> graph = Graph.fromDataSet(vertices, edges, env);

        DataSet<Vertex<Long,Double>> singleSourceShortestPaths =
                graph.run(new SingleSourceShortestPaths<Long>(srcVertexId, maxIterations)).getVertices();

        // emit the result
        if(fileOutput) {
            singleSourceShortestPaths.writeAsCsv(outputPath, "\n", " ");
        } else {
            singleSourceShortestPaths.print();
        }

        env.execute();
    }

    @Override
    public String getDescription() {
        return "Single Source Shortest Paths";
    }
}
