package flink.graphs;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.apache.flink.util.Collector;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

@RunWith(Parameterized.class)
public class TestJoinWithVertices extends JavaProgramTestBase {

    private static int NUM_PROGRAMS = 2;

    private int curProgId = config.getInteger("ProgramId", -1);
    private String resultPath;
    private String expectedResult;

    public TestJoinWithVertices(Configuration config) {
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

        @SuppressWarnings("serial")
        public static String runProgram(int progId, String resultPath) throws Exception {

            switch (progId) {
                case 1: {
				/*
				 * Test joinWithVertices with the input DataSet parameter identical
				 * to the vertex DataSet
				 */
                    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

                    Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
                            TestGraphUtils.getLongLongEdgeData(env), env);

                    Graph<Long, Long, Long> result = graph.joinWithVertices(graph.getVertexIds()
                                    .map(new MapFunction<Long, Tuple1<Long>>() {
                                        @Override
                                        public Tuple1<Long> map(Long aLong) throws Exception {
                                            return new Tuple1<Long>(aLong);
                                        }
                                    }),
                            new MapFunction<Long, Long>() {
                                @Override
                                public Long map(Long aLong) throws Exception {
                                    return new Long(aLong * 2);
                                }
                            });

                    result.getVertices().writeAsCsv(resultPath);
                    env.execute();

                    return "1,2\n" +
                            "2,4\n" +
                            "3,6\n" +
                            "4,8\n" +
                            "5,10\n";
                }

                case 2: {
				/*
				 * Test joinWithVertices
				 */
                    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

                    Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
                            TestGraphUtils.getLongLongEdgeData(env), env);

                    Graph<Long, Long, Long> result = graph.joinWithVertices(graph.getVertexIds().first(3)
                                    .map(new MapFunction<Long, Tuple1<Long>>() {
                                        @Override
                                        public Tuple1<Long> map(Long aLong) throws Exception {
                                            return new Tuple1<Long>(aLong);
                                        }
                                    }),
                            new MapFunction<Long, Long>() {
                                @Override
                                public Long map(Long aLong) throws Exception {
                                    return new Long(aLong * 2);
                                }
                            });

                    result.getVertices().writeAsCsv(resultPath);
                    env.execute();

                    return "1,2\n" +
                            "2,4\n" +
                            "3,6\n" +
                            "4,4\n" +
                            "5,5\n";

                }
                default:
                    throw new IllegalArgumentException("Invalid program id");
            }
        }
    }
}