package stream.scotty.flinkBenchmark;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

public class BufferUntilNbMinThenBurstOnce<T> extends RichFlatMapFunction<T, T> {

    private final int nbMin;

    private transient ArrayList<T> buffer;
    private transient boolean started;   // after first burst
    private transient boolean measured;  // measure initial throughput once

    // START of initial phase: first element buffered
    private transient long firstBufferedNs;

    public BufferUntilNbMinThenBurstOnce(int nbMin) {
        this.nbMin = nbMin;
    }

    @Override
    public void open(Configuration parameters) {
        buffer = new ArrayList<>(nbMin);
        started = false;
        measured = false;
        firstBufferedNs = -1;
    }

    @Override
    public void flatMap(T value, Collector<T> out) {

        // Phase 1: buffer until nbMin, then burst once
        if (!started) {

            if (buffer.isEmpty()) {
                firstBufferedNs = System.nanoTime(); // start time
            }

            buffer.add(value);

            if (buffer.size() >= nbMin) {
                started = true;

                // emit burst
                for (T e : buffer) {
                    out.collect(e);
                }

                long endNs = System.nanoTime(); // stop time

                if (!measured) {
                    double sec = (endNs - firstBufferedNs) / 1_000_000_000.0;
                    double tp = (sec > 0) ? (buffer.size() / sec) : 0.0;

                    ThroughputStatistics.getInstance().addThrouputResult(tp);

                    // debit starts AFTER initial phase is finished
                    DebitedThroughputStatistics.getInstance().start(endNs);

                    measured = true;
                }

                buffer.clear();
            }
            return;
        }

        // Phase 2: after burst, everything counts as debited throughput
        DebitedThroughputStatistics.getInstance().increment();
        out.collect(value);
    }
}
