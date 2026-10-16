package stream.scotty.flinkBenchmark;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.api.java.tuple.Tuple4;

public class WindowLatencyMap implements MapFunction<Tuple4<String,Integer,Long,Long>, Tuple4<String,Integer,Long,Long>> {

    @Override
    public Tuple4<String,Integer,Long,Long> map(Tuple4<String,Integer,Long,Long> value) {

        long now = System.currentTimeMillis();
        long windowEnd = value.f3; // ou ctx.window().getEnd() si exposé
        long latencyMs = Math.max(0, now - windowEnd);

        LatencyStatistics.getInstance().addLatencyMs(latencyMs);
        return value; // on ne change rien
    }
}
