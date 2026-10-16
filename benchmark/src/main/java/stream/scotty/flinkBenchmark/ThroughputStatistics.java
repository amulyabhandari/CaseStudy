package stream.scotty.flinkBenchmark;

public class ThroughputStatistics {

    private static ThroughputStatistics statistics;
    private boolean pause;

    private double counter = 0;
    private double sum = 0;

    private ThroughputStatistics() {}

    public static ThroughputStatistics getInstance() {
        if (statistics == null) {
            statistics = new ThroughputStatistics();
        }
        return statistics;
    }

    public void addThrouputResult(double throuputPerS) {
        if (this.pause) return;
        counter++;
        sum += throuputPerS;
    }

    public void clean() {
        counter = 0;
        sum = 0;
        pause = false;
    }

    public double mean() {
        return (counter > 0) ? (sum / counter) : 0.0;
    }

    @Override
    public String toString() {
        return "\nThroughput Mean: " + ((counter > 0) ? (sum / counter) : 0.0);
    }

    public void pause(final boolean pause) {
        this.pause = pause;
    }
}
