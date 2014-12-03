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
public class TestGetNeighborhoodGraph extends JavaProgramTestBase {

    private static int NUM_PROGRAMS = 2;

    private int curProgId = config.getInteger("ProgramId", -1);
    private String resultPath;
    private String expectedResult;

    public TestGetNeighborhoodGraph(Configuration config) {
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

        public static String runProgram(int progId, String resultPath) throws Exception {

            switch (progId) {
                case 1: {
				/*
				 * Test getNeighborhood with 1 step
				 */
                    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

                    Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
                            TestGraphUtils.getLongLongEdgeData(env), env);

                    graph.getNeighborhoodGraph(1L, 1).getVertexIds().writeAsText(resultPath);
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

                        graph.getNeighborhoodGraph(1L, 2).getVertexIds().writeAsText(resultPath);
                        env.execute();
                        return "1\n" +
                                "2\n" +
                                "3\n" +
                                "4\n" +
                                "5\n";
                }
                case 3: {
                }
                case 4: {
                }
                case 5: {
                }
                case 6: {
                }
                default:
                    throw new IllegalArgumentException("Invalid program id");

            }
        }
    }
}
