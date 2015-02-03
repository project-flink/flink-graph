package flink.graphs.example.utils;

import org.apache.flink.api.common.aggregators.Aggregator;
import org.apache.flink.types.BooleanValue;

@SuppressWarnings("serial")
public class PointerJumpingAggregator implements Aggregator<BooleanValue> {

    private boolean converges = false;

    @Override
    public BooleanValue getAggregate() {
        return new BooleanValue(converges);
    }

    @Override
    public void aggregate(BooleanValue booleanValue) {
        converges = booleanValue.getValue();
    }

    @Override
    public void reset() {
        converges = false;
    }
}
