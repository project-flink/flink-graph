package flink.graphs.example.utils;

public class MinSpanningTreeData {

    public static final int NUM_VERTICES = 10;

    public static final String VERTICES = 	"1,A\n" +
                                            "2,B\n" +
                                            "3,C\n" +
                                            "4,D\n" +
                                            "5,E\n" +
                                            "6,F\n" +
                                            "7,G\n" +
                                            "8,H\n" +
                                            "9,I\n" +
                                            "10,J";

    public static final String EDGES =  "1,2,9.0\n" +
                                        "1,4,2.0\n" +
                                        "1,6,1.0\n" +
                                        "2,3,3.0\n" +
                                        "2,4,8.0\n" +
                                        "3,5,7.0\n" +
                                        "4,5,12.0\n" +
                                        "4,6,13.0\n" +
                                        "4,7,5.0\n" +
                                        "5,7,14.0\n" +
                                        "5,8,6.0\n" +
                                        "6,7,10.0\n" +
                                        "7,8,15.0\n" +
                                        "9,10,4.0";

    public static final String RESULTED_MIN_SPANNING_TREE = "1,4,2.0\n" +
                                                            "1,6,1.0\n" +
                                                            "2,3,3.0\n" +
                                                            "2,4,8.0\n" +
                                                            "3,5,7.0\n" +
                                                            "4,7,5.0\n" +
                                                            "5,8,6.0\n" +
                                                            "9,10,4.0";

    private MinSpanningTreeData() {}
}
