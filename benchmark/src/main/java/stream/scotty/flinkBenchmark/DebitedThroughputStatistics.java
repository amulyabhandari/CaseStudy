package stream.scotty.flinkBenchmark;

public class DebitedThroughputStatistics {

    private static DebitedThroughputStatistics instance;

    private long startTimeNs = -1;
    private long stopTimeNs = -1;
    private long totalEvents = 0;

    private boolean started = false;
    private boolean stopped = false;

    public static DebitedThroughputStatistics getInstance() {
        if (instance == null) instance = new DebitedThroughputStatistics();
        return instance;
    }

    public void start(long startTimeNs) {
        if (!started) {
            this.startTimeNs = startTimeNs;
            this.started = true;
        }
    }

    public void stop(long stopTimeNs) {
        if (started && !stopped) {
            this.stopTimeNs = stopTimeNs;
            this.stopped = true;
        }
    }

    public void increment() {
        if (started && !stopped) totalEvents++;
    }

    public double computeEventsPerSecond() {
        if (!started || startTimeNs <= 0) return 0.0;

        long end = stopped ? stopTimeNs : System.nanoTime();
        if (end <= startTimeNs) return 0.0; // IMPORTANT

        double sec = (end - startTimeNs) / 1_000_000_000.0;
        return (sec > 0) ? (totalEvents / sec) : 0.0;
    }

    public void reset() {
        startTimeNs = -1;
        stopTimeNs = -1;
        totalEvents = 0;
        started = false;
        stopped = false;
    }

    @Override
    public String toString() {
        return "\nThroughput debited: " + computeEventsPerSecond() + " evts/s";
    }
}
