package stream.scotty.flinkBenchmark;

public class LatencyStatistics {

    private static LatencyStatistics statistics;
    private boolean pause;

    private LatencyStatistics() {}

    private double counter = 0;
    private double sumMs = 0;

    public static LatencyStatistics getInstance() {
        if (statistics == null) statistics = new LatencyStatistics();
        return statistics;
    }

    public void addLatencyMs(double latencyMs) {
        if (pause) return;
        counter++;
        sumMs += latencyMs;
    }

    public void clean() {
        counter = 0;
        sumMs = 0;
    }

    public double meanMs() {
        return counter == 0 ? 0.0 : (sumMs / counter);

    }

    @Override
    public String toString() {
        return "\nLatency Mean (ms): " + this.meanMs();
    }

    public void pause(final boolean pause) {
        this.pause = pause;
    }
}
