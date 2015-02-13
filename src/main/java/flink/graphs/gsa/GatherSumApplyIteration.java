/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package flink.graphs.gsa;

import flink.graphs.Edge;
import flink.graphs.Vertex;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.io.Serializable;

public abstract class GatherSumApplyIteration<VertexKey extends Comparable<VertexKey> & Serializable,
        VertexValue extends Serializable> implements CustomUnaryOperation<Vertex<VertexKey, VertexValue>,
        Vertex<VertexKey, VertexValue>> {

    private DataSet<Vertex<VertexKey, VertexValue>> dataSet;
    private final int maximumNumberOfIterations;

    public GatherSumApplyIteration(int maxIterations) {
        this.maximumNumberOfIterations = maxIterations;
    }

    @Override
    public void setInput(DataSet<Vertex<VertexKey, VertexValue>> dataSet) {
        this.dataSet = dataSet;
    }

    @Override
    public DataSet<Vertex<VertexKey, VertexValue>> createResult() {
        if (dataSet == null) {
            throw new IllegalStateException("The input data set has not been set.");
        }

        final int[] zeroKeyPos = new int[] {0};
        final DeltaIteration<Vertex<VertexKey, VertexValue>, Vertex<VertexKey, VertexValue>> iteration =
                dataSet.iterateDelta(dataSet, maximumNumberOfIterations, zeroKeyPos);

        return null;
    }
}
