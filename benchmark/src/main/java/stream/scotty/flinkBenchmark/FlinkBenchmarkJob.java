package stream.scotty.flinkBenchmark;

import stream.scotty.core.windowType.*;
import stream.scotty.core.windowType.Window;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.*;
import org.apache.flink.streaming.api.*;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.*;
import org.apache.flink.streaming.api.functions.sink.*;
import org.apache.flink.streaming.api.windowing.time.*;
import org.apache.flink.streaming.api.windowing.windows.*;

import java.util.*;

public class FlinkBenchmarkJob {

    public FlinkBenchmarkJob(
            List<Window> assigners,
            StreamExecutionEnvironment env,
            long runtime,
            int throughput,
            List<Tuple2<Long, Long>> gaps
    ) {
        Map<String, String> configMap = new HashMap<>();
        ParameterTool parameters = ParameterTool.fromMap(configMap);

        env.getConfig().setGlobalJobParameters(parameters);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        env.setMaxParallelism(1);

        DataStream<Tuple4<String, Integer, Long, Long>> messageStream = env
                .addSource(new LoadGeneratorSource(runtime, throughput, gaps));

        int NB_MIN = 100000;

        DataStream<Tuple4<String, Integer, Long, Long>> buffered = messageStream
                .flatMap(new BufferUntilNbMinThenBurstOnce<>(NB_MIN));

        final SingleOutputStreamOperator<Tuple4<String, Integer, Long, Long>> timestampsAndWatermarks = buffered
                .assignTimestampsAndWatermarks(new BenchmarkJob.TimestampsAndWatermarks());

        KeyedStream<Tuple4<String, Integer, Long, Long>, Tuple> keyedStream =
                timestampsAndWatermarks.keyBy(0);

        for (Window w : assigners) {

            if (w instanceof TumblingWindow) {
                WindowedStream<Tuple4<String, Integer, Long, Long>, Tuple, TimeWindow> tw =
                        keyedStream.timeWindow(Time.milliseconds(((TumblingWindow) w).getSize()));

                tw.sum(1).addSink(new SinkFunction<Tuple4<String, Integer, Long, Long>>() {
                    @Override
                    public void invoke(final Tuple4<String, Integer, Long, Long> value) throws Exception {
                        // no-op
                    }
                });
            }

            if (w instanceof SlidingWindow) {
                WindowedStream<Tuple4<String, Integer, Long, Long>, Tuple, TimeWindow> tw =
                        keyedStream.timeWindow(
                                Time.milliseconds(((SlidingWindow) w).getSize()),
                                Time.milliseconds(((SlidingWindow) w).getSlide())
                        );

                tw.aggregate(new SumAgg(), new WindowLatencyProc())
                        .addSink(new SinkFunction<Integer>() {
                            @Override
                            public void invoke(final Integer value) throws Exception {
                                // no-op
                            }
                        });
            }
        }

        try {
            env.execute("Scotty-Flink-Benchmark");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            DebitedThroughputStatistics.getInstance().stop(System.nanoTime());
            System.out.println(ThroughputStatistics.getInstance());
            System.out.println(DebitedThroughputStatistics.getInstance());
        }
    }
}
