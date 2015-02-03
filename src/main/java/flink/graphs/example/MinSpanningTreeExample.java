package flink.graphs.example;

import flink.graphs.Edge;
import flink.graphs.Graph;
import flink.graphs.Vertex;
import flink.graphs.library.MinSpanningTree;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class MinSpanningTreeExample implements ProgramDescription {

    public static void main (String [] args) throws Exception {

        if(!parseParameters(args)) {
            return;
        }

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Vertex<Long, String>> vertices = getVerticesDataSet(env);

        DataSet<Edge<Long, Double>> edges = getEdgesDataSet(env);

        Graph<Long, String, Double> graph = Graph.fromDataSet(vertices, edges, env);

        DataSet<Edge<Long, Double>> minimumSpanningTree = graph.run(new MinSpanningTree(maxIterations)).getEdges();

        // emit result
        if(fileOutput) {
            minimumSpanningTree.writeAsCsv(outputPath, "\n", ",");
        } else {
            minimumSpanningTree.print();
        }

        minimumSpanningTree.getExecutionEnvironment().execute("Executing MinSpanningTree Example");
    }

    @Override
    public String getDescription() {
        return "A Parallel Version of Boruvka's Minimum Spanning Tree Algorithm";
    }

    //******************************************************************************************************************
    // UTIL METHODS
    //******************************************************************************************************************

    private static boolean fileOutput = false;
    private static String verticesInputPath = null;
    private static String edgesInputPath = null;
    private static String outputPath = null;
    private static int maxIterations = 10;

    private static boolean parseParameters(String[] args) {

        if(args.length > 0) {
            if(args.length == 4) {
                fileOutput = true;
                verticesInputPath = args[0];
                edgesInputPath = args[1];
                outputPath = args[2];
                maxIterations = Integer.parseInt(args[3]);
            } else {
                System.err.println("Usage: MinSpanningTree <input vertices path> <input edges path> <output path> " +
                        "<num iterations>");
                return false;
            }
        }
        return true;
    }

    private static DataSet<Vertex<Long, String>> getVerticesDataSet(ExecutionEnvironment env) {
        if(fileOutput) {
            return env
                    .readCsvFile(verticesInputPath)
                    .lineDelimiter("\n")
                    .types(Long.class, String.class)
                    .map(new MapFunction<Tuple2<Long, String>, Vertex<Long, String>>() {
                        @Override
                        public Vertex<Long, String> map(Tuple2<Long, String> tuple2) throws Exception {
                            return new Vertex<Long, String>(tuple2.f0, tuple2.f1);
                        }
                    });
        } else {
            System.err.println("Usage: MinSpanningTree <input vertices path> <input edges path> <output path> " +
                    "<num iterations>");
            return null;
        }
    }

    private static DataSet<Edge<Long, Double>> getEdgesDataSet(ExecutionEnvironment env) {
        if(fileOutput) {
            return env
                    .readCsvFile(edgesInputPath)
                    .lineDelimiter("\n")
                    .types(Long.class, Long.class, Double.class)
                    .map(new MapFunction<Tuple3<Long, Long, Double>, Edge<Long, Double>>() {
                        @Override
                        public Edge<Long, Double> map(Tuple3<Long, Long, Double> tuple3) throws Exception {
                            return new Edge(tuple3.f0, tuple3.f1, tuple3.f2);
                        }
                    });
        } else {
            System.err.println("Usage: MinSpanningTree <input vertices path> <input edges path> <output path> " +
                    "<num iterations>");
            return null;
        }
    }
}
