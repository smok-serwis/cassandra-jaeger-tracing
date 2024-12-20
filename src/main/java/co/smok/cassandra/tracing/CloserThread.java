package co.smok.cassandra.tracing;

import io.jaegertracing.internal.clock.Clock;
import io.jaegertracing.internal.clock.SystemClock;

import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

/**
 * Since Cassandra does not close traces made by nodes responding to the coordinator,
 * we need to close them manually.
 * This will wait until WAIT_FOR_EVENTS_IN_US microseconds have passed since the
 * last trace() and close the parent span manually, with the timestamp of it's last trace.
 */
public class CloserThread extends Thread {

    protected static final long WAIT_FOR_EVENTS_IN_US = 30000000;  // 30 seconds
    private static final Clock clock = new SystemClock();
    private final List<JaegerTraceState> to_close = new LinkedList<>();
    public static CloserThread instance = null;

    public CloserThread() {
        super("TraceCloser");
        setDaemon(true);
        CloserThread.instance = this;
    }

    private boolean shouldExpire(JaegerTraceState trace) {
        return clock.currentTimeMicros() - trace.timestamp > (WAIT_FOR_EVENTS_IN_US);
    }

    public synchronized void publish(JaegerTraceState trace) {
        this.to_close.add(trace);
    }

    public synchronized void process() {
        ListIterator<JaegerTraceState> iter = to_close.listIterator();
        while(iter.hasNext()) {
            JaegerTraceState trace = iter.next();
            if (shouldExpire(trace)) {
                trace.stop();
                iter.remove();
            }
        }
    }

    @Override
    public void run() {
        while (true) {
            this.process();
            try {
                sleep(1000000);
            } catch (InterruptedException e) {
            }
        }
    }
}