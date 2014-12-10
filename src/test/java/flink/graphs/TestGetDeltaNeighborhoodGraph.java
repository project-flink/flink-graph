package flink.graphs;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;

@RunWith(Parameterized.class)
public class TestGetDeltaNeighborhoodGraph extends JavaProgramTestBase {

    private static int NUM_PROGRAMS = 7;

    private int curProgId = config.getInteger("ProgramId", -1);
    private String resultPath;
    private String expectedResult;

    public TestGetDeltaNeighborhoodGraph(Configuration config) {
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

        for(int i=1; i <= NUM_PROGRAMS; i++) {
            Configuration config = new Configuration();
            config.setInteger("ProgramId", i);
            tConfigs.add(config);
        }

        return toParameterList(tConfigs);
    }

    private static class GraphProgs {

    	// test with distance 0
    	// distinguish in- and out- cases
        public static String runProgram(int progId, String resultPath) throws Exception {

            switch (progId) {
                case 1: {
				/*
				 * Test getNeighborhood with 1 step
				 */
                    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

                    Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
                            TestGraphUtils.getLongLongEdgeData(env), env);

                    graph.getDeltaNeighborhoodGraph(1L, 1).getVertexIds().writeAsText(resultPath);
                    env.execute();
                    return "1\n" +
                            "2\n" +
                            "3\n" +
                            "5\n";
                }
                case 2: {
                	/*
    				 * Test getNeighborhood with 2 steps
    				 */
                        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

                        Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
                                TestGraphUtils.getLongLongEdgeData(env), env);

                        graph.getDeltaNeighborhoodGraph(1L, 2).getVertexIds().writeAsText(resultPath);
                        env.execute();
                        return "1\n" +
                                "2\n" +
                                "3\n" +
                                "4\n" +
                                "5\n";
                }
                case 3: {
                	/*
    				 * Test getNeighborhood with 1 step: check edges
    				 */
                        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

                        Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
                                TestGraphUtils.getLongLongEdgeData(env), env);

                        graph.getDeltaNeighborhoodGraph(1L, 1).getEdgeIds().writeAsCsv(resultPath);
                        env.execute();
                        return "1,2\n" +
                                "1,3\n" +
                                "5,1\n";
                }
                case 4: {
                	/*
    				 * Test getNeighborhood with 2 steps: check edges
    				 */
                        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

                        Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
                                TestGraphUtils.getLongLongEdgeData(env), env);

                        graph.getDeltaNeighborhoodGraph(1L, 2).getEdgeIds().writeAsCsv(resultPath);
                        env.execute();
                        return "1,2\n" +
                        		"1,3\n" +
                                "2,3\n" +
                                "3,4\n" +
                                "3,5\n" +
                                "4,5\n" +
                                "5,1\n";
                }
                case 5: {
                	/*
    				 * Test getNeighborhood with 2 steps: check edges with bigger graph
    				 */
                        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

                        Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getMoreLongLongVertexData(env),
                        		TestGraphUtils.getMoreLongLongEdgeData(env),
                        		env);

                        graph.getDeltaNeighborhoodGraph(1L, 2).getEdgeIds().writeAsCsv(resultPath);
                        env.execute();
                        return "1,2\n" +
                        		"1,3\n" +
                                "2,3\n" +
                                "3,4\n" +
                                "3,5\n" +
                                "4,5\n" +
                                "5,1\n" +
                                "5,6\n" +
                                "7,5\n";
                }
                case 6: {
                	/*
    				 * Test getNeighborhood with 2 steps: check vertices with bigger graph
    				 */
                        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

                        Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getMoreLongLongVertexData(env),
                        		TestGraphUtils.getMoreLongLongEdgeData(env),
                        		env);

                        graph.getDeltaNeighborhoodGraph(1L, 2).getVertexIds().writeAsText(resultPath);
                        env.execute();
                        return "1\n" +
		                        "2\n" +
		                        "3\n" +
		                        "4\n" +
		                        "5\n" +
		                        "6\n" +
		                        "7\n";
                }
                case 7: {
                	/*
    				 * Test getNeighborhood with 3 steps: neighborhood-3 is the whole graph
    				 */
                        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

                        Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getMoreLongLongVertexData(env),
                        		TestGraphUtils.getMoreLongLongEdgeData(env),
                        		env);

                        graph.getDeltaNeighborhoodGraph(1L, 3).getEdgeIds().writeAsCsv(resultPath);
                        env.execute();
                        return "1,2\n" +
                        		"1,3\n" +
                                "2,3\n" +
                                "3,4\n" +
                                "3,5\n" +
                                "4,5\n" +
                                "4,8\n" +
                                "4,9\n" +
                                "5,1\n" +
                                "5,6\n" +
                                "6,7\n" +
                                "7,5\n";
                }
                default:
                    throw new IllegalArgumentException("Invalid program id");
            }
        }
    }
}
