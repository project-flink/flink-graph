package flink.graphs;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

@RunWith(Parameterized.class)
public class TestVertexOperations extends JavaProgramTestBase {
    private static int NUM_PROGRAMS = 10;

    private int curProgId = config.getInteger("ProgramId", -1);
    private String resultPath;
    private String expectedResult;

    public TestVertexOperations(Configuration config) {
        super(config);
    }

    @Override
    protected void preSubmit() throws Exception {
        resultPath = getTempDirPath("result");
    }

    @Override
    protected void testProgram() throws Exception {
        expectedResult = GraphProgs.runProgram(curProgId, resultPath);
    }

    @Override
    protected void postSubmit() throws Exception {
        compareResultsByLinesInMemory(expectedResult, resultPath);
    }

    @Parameterized.Parameters
    public static Collection<Object[]> getConfigurations() throws FileNotFoundException, IOException {

        LinkedList<Configuration> tConfigs = new LinkedList<Configuration>();

        for (int i = 1; i <= NUM_PROGRAMS; i++) {
            Configuration config = new Configuration();
            config.setInteger("ProgramId", i);
            tConfigs.add(config);
        }

        return toParameterList(tConfigs);
    }

    private static class GraphProgs {

        @SuppressWarnings("serial")
        public static String runProgram(int progId, String resultPath) throws Exception {
            switch(progId) {
                case 1: {
					/*
					 * Test inDegree:
					 */
                    Integer desiredVertexPos = 3;
                    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

                    Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
                            TestGraphUtils.getLongLongEdgeData(env));
                    List<Tuple2<Long, Long>> data = TestGraphUtils.vertexInputData(graph, env);
                    Vertex vertex = new Vertex(data.get(desiredVertexPos).f0, data.get(desiredVertexPos).f1);
                    DataSet<Long> result =  vertex.inDegree(env, graph);
                    result.writeAsText(resultPath);
                    env.execute();

                    return "1\n";
                }
                case 2: {
					/*
					 * Test inDegree for a vertex that has no ingoing edge:
					 */
                    Integer desiredVertexPos = 0;
                    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

                    Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData2(env),
                            TestGraphUtils.getLongLongEdgeData2(env));
                    List<Tuple2<Long, Long>> data = TestGraphUtils.vertexInputData(graph, env);
                    Vertex vertex = new Vertex(data.get(desiredVertexPos).f0, data.get(desiredVertexPos).f1);
                    DataSet<Long> result =  vertex.inDegree(env, graph);
                    result.writeAsText(resultPath);
                    env.execute();

                    return "0";
                }
                case 3: {
					/*
					 * Test outDegree:
					 */
                    Integer desiredVertexPos = 1;
                    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

                    Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
                            TestGraphUtils.getLongLongEdgeData(env));
                    List<Tuple2<Long, Long>> data = TestGraphUtils.vertexInputData(graph, env);
                    Vertex vertex = new Vertex(data.get(desiredVertexPos).f0, data.get(desiredVertexPos).f1);
                    DataSet<Long> result =  vertex.outDegree(env, graph);
                    result.writeAsText(resultPath);
                    env.execute();

                    return "1\n";
                }

                case 4: {
					/*
					 * Test outDegree for a vertex that has no outgoing edge:
					 */
                    Integer desiredVertexPos = 4;
                    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

                    Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData2(env),
                            TestGraphUtils.getLongLongEdgeData2(env));
                    List<Tuple2<Long, Long>> data = TestGraphUtils.vertexInputData(graph, env);
                    Vertex vertex = new Vertex(data.get(desiredVertexPos).f0, data.get(desiredVertexPos).f1);
                    DataSet<Long> result =  vertex.outDegree(env, graph);
                    result.writeAsText(resultPath);
                    env.execute();

                    return "0";
                }

                case 5: {
					/*
					 * Test getDegree:
					 */
                    Integer desiredVertexPos = 0;
                    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

                    Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData2(env),
                            TestGraphUtils.getLongLongEdgeData2(env));
                    List<Tuple2<Long, Long>> data = TestGraphUtils.vertexInputData(graph, env);
                    Vertex vertex = new Vertex(data.get(desiredVertexPos).f0, data.get(desiredVertexPos).f1);
                    DataSet<Long> result =  vertex.getDegree(env, graph);
                    result.writeAsText(resultPath);
                    env.execute();

                    return "2\n";
                }
                case 6: {
					/*
					 * Test getInNeighbours:
					 */
                    Integer desiredVertexPos = 2;
                    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

                    Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
                            TestGraphUtils.getLongLongEdgeData(env));
                    List<Tuple2<Long, Long>> data = TestGraphUtils.vertexInputData(graph, env);
                    Vertex vertex = new Vertex(data.get(desiredVertexPos).f0, data.get(desiredVertexPos).f1);
                    DataSet<Tuple2<Long, Long>> result =  vertex.getInNeighbors(graph);
                    result.writeAsCsv(resultPath);
                    env.execute();

                    return "1,1\n"+
                            "2,2\n";
                }
                case 7: {
					/*
					 * Test getInNeighbours for a vertex that does not have a ingoing edge.
					 */
                    Integer desiredVertexPos = 0;
                    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

                    Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData2(env),
                            TestGraphUtils.getLongLongEdgeData2(env));
                    List<Tuple2<Long, Long>> data = TestGraphUtils.vertexInputData(graph, env);
                    Vertex vertex = new Vertex(data.get(desiredVertexPos).f0, data.get(desiredVertexPos).f1);
                    DataSet<Tuple2<Long, Long>> result =  vertex.getInNeighbors(graph);
                    result.writeAsCsv(resultPath);
                    env.execute();

                    return "";
                }
                case 8: {
					/*
					 * Test getOutNeighbours:
					 */
                    Integer desiredVertexPos = 0;
                    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

                    Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
                            TestGraphUtils.getLongLongEdgeData(env));
                    List<Tuple2<Long, Long>> data = TestGraphUtils.vertexInputData(graph, env);
                    Vertex vertex = new Vertex(data.get(desiredVertexPos).f0, data.get(desiredVertexPos).f1);
                    DataSet<Tuple2<Long, Long>> result =  vertex.getOutNeighbors(graph);
                    result.writeAsCsv(resultPath);
                    env.execute();

                    return "2,2\n"+
                            "3,3\n";
                }
                case 9: {
                    /*
					 * Test getOutNeighbours for a vertex that has no outgoing edge:
					 */
                    Integer desiredVertexPos = 4;
                    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

                    Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData2(env),
                            TestGraphUtils.getLongLongEdgeData2(env));
                    List<Tuple2<Long, Long>> data = TestGraphUtils.vertexInputData(graph, env);
                    Vertex vertex = new Vertex(data.get(desiredVertexPos).f0, data.get(desiredVertexPos).f1);
                    DataSet<Tuple2<Long, Long>> result =  vertex.getOutNeighbors(graph);
                    result.writeAsCsv(resultPath);
                    env.execute();

                    return "";
                }
                case 10: {
                    /*
                     * Test getAllNeighbours:
                     */
                    Integer desiredVertexPos = 0;
                    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

                    Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
                            TestGraphUtils.getLongLongEdgeData(env));
                    List<Tuple2<Long, Long>> data = TestGraphUtils.vertexInputData(graph, env);
                    Vertex vertex = new Vertex(data.get(desiredVertexPos).f0, data.get(desiredVertexPos).f1);
                    DataSet<Tuple2<Long, Long>> result =  vertex.getAllNeighbors(graph);
                    result.writeAsCsv(resultPath);
                    env.execute();

                    return "2,2\n"+
                            "3,3\n"+
                            "5,5\n";
                }

                default:
                    throw new IllegalArgumentException("Invalid program id");
            }
        }
    }
}