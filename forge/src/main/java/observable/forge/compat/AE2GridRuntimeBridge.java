package observable.forge.compat;

import observable.CompatProfilerReport;
import observable.Observable;
import observable.Props;
import observable.server.Profiler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Connects the AE2 compatibility profiler directly to Observable's profiling
 * lifecycle. v20.6.1 remains opt-in: regular Observable runs never start this
 * lifecycle. Explicit AE2 runs export bounded Top-N detail and publish one
 * inclusive virtual marker for each exported grid.
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

        // This hook only runs for /observable ae2 ... . Write the bounded standalone
        // report first, while blockTimingsMap still contains only real physical
        // timing buckets. Then materialize one virtual Grid marker per requested
        // Top-N grid for the normal Observable upload.
        Props.compatProfilerSnapshotHook = () -> {
            try {
                Profiler profiler = Observable.INSTANCE.getPROFILER();
                Path jsonFile = AE2GridProfiler.writeDetailedReport(profiler.getLastCompletedTicks());
                Path htmlFile = null;
                if (jsonFile != null) {
                    String jsonName = jsonFile.getFileName().toString();
                    String baseName = jsonName.endsWith(".json")
                            ? jsonName.substring(0, jsonName.length() - 5)
                            : jsonName;
                    Path candidate = jsonFile.resolveSibling(baseName + ".html");
                    if (Files.isRegularFile(candidate)) {
                        htmlFile = candidate;
                    }
                }
                AE2GridProfiler.publishVirtualTimings();
                return new CompatProfilerReport(jsonFile, htmlFile);
            } finally {
                CompatTiming.onAE2CompatSessionEnd();
            }
        };

        // Before the packet is made for the in-game overlay, normalize any old
        // AE2 virtual markers and leave one inclusive marker per requested Top-N grid.
        Props.compatProfilerClientViewHook = AE2GridProfiler::prepareClientOverlay;

        LOGGER.info("Observable AE2 Grid profiler bridge enabled (v20.6.1 Direct Report Download Hardcoded Chat + v20.5.3 Administration Consistency Guard + v20.5.2 Administration Accuracy + retained v20.4.1 Diagnostic Intelligence + Physical Grid Aggregate + Secondary Foreign Dispatch + Robust Burst Detection + v20.3.2.16 Drive Hot Path Cache + ExtendedAE Mount Ownership + On-demand AE2 + 4-tick Scout Top-N Runtime Detail + Compact Reports + Large Server Safety)");
    }
}
