package stream.scotty.flinkBenchmark;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple4;

public class SumAgg implements AggregateFunction<
        Tuple4<String, Integer, Long, Long>,  // IN
        Integer,                              // ACC
        Integer                               // OUT (value before window function)
        > {

    @Override
    public Integer createAccumulator() {
        return 0;
    }

    @Override
    public Integer add(Tuple4<String, Integer, Long, Long> value, Integer acc) {
        return acc + value.f1; // sum sur champ 1
    }

    @Override
    public Integer getResult(Integer acc) {
        return acc;
    }

    @Override
    public Integer merge(Integer a, Integer b) {
        return a + b;
    }
}
