package stream.scotty.flinkBenchmark;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WindowLatencyProc extends ProcessWindowFunction<
        Integer,     // IN  (sortie de SumAgg)
        Integer,     // OUT (ce qu’on envoie ensuite au sink)
        Tuple,       // KEY
        TimeWindow   // WINDOW
        > {

    @Override
    public void process(Tuple key,
                        Context ctx,
                        Iterable<Integer> values,
                        Collector<Integer> out) {

        Integer sum = values.iterator().next(); // aggregate donne 1 seule valeur

        long now = System.currentTimeMillis();
        long windowEnd = ctx.window().getEnd();

        long latencyMs = now - windowEnd;
        if (latencyMs < 0) latencyMs = 0;

        LatencyStatistics.getInstance().addLatencyMs(latencyMs);

        out.collect(sum); // on laisse passer le résultat normal
    }
}
