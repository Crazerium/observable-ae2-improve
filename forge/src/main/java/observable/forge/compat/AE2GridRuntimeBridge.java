package observable.forge.compat;

import observable.Observable;
import observable.Props;
import observable.server.Profiler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Connects the AE2 compatibility profiler directly to Observable's profiling
 * lifecycle. v20 keeps the full grid breakdown for JSON/HTML/web reports while the
 * in-game overlay is collapsed to a single total marker per AE2 grid.
 */
public final class AE2GridRuntimeBridge {
    private static final Logger LOGGER = LogManager.getLogger("Observable/AE2Grid");
    private static final AtomicBoolean REGISTERED = new AtomicBoolean();

    private AE2GridRuntimeBridge() {
    }

    public static void register() {
        if (!REGISTERED.compareAndSet(false, true)) {
            return;
        }

        Props.compatProfilerStartHook = () -> {
            AE2GridProfiler.onSessionStart();
            CompatTiming.onAE2CompatSessionStart();
        };

        // This hook runs before Observable snapshots/uploads the profile. Write the
        // standalone report first, while blockTimingsMap still contains only real
        // physical timing buckets. Virtual Grid markers are materialized afterwards
        // for the normal Observable upload, so they cannot masquerade as a physical
        // device when a reserved marker coordinate later collides with a real block.
        Props.compatProfilerSnapshotHook = () -> {
            try {
                Profiler profiler = Observable.INSTANCE.getPROFILER();
                AE2GridProfiler.writeDetailedReport(profiler.getLastCompletedTicks());
                AE2GridProfiler.publishVirtualTimings();
            } finally {
                CompatTiming.onAE2CompatSessionEnd();
            }
        };

        // The upload above already captured the full diagnostic profile. Before
        // the packet is made for the in-game overlay, remove the diagnostic
        // tower and leave only one inclusive total marker per grid.
        Props.compatProfilerClientViewHook = AE2GridProfiler::prepareClientOverlay;

        LOGGER.info("Observable AE2 Grid profiler bridge enabled (v20.3.2.7 Typed Cell Owner Resolution + Drive Owner Mapping + Drive Coverage Diagnostics + Profiler Correctness + Large Server Safety + Distribution Modes + report retention + Operation-aware Spike Analysis + Current Diagnosis + Regression Intelligence + duplicate-anchor-safe Compare + moderator TP)");
    }
}
