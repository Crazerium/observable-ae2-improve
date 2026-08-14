package observable;

import observable.server.Profiler;

import java.util.concurrent.atomic.AtomicReference;

public class Props {
    public static volatile boolean notProcessing = true;

    public static AtomicReference<Profiler.TimingData> currentTarget = new AtomicReference<>(null);

    // Optional loader-specific profiler lifecycle hooks. Forge compat uses these
    // to reset and snapshot AE2 grid-wide metrics at exactly the same boundaries
    // as Observable's own profiling session.
    public static volatile Runnable compatProfilerStartHook = null;
    public static volatile Runnable compatProfilerSnapshotHook = null;
    // Runs after the full profile has been saved/uploaded, just before the
    // result packet is built for the in-game client. Compat layers can use
    // this to collapse diagnostic-only virtual markers without removing them
    // from the uploaded profile.
    public static volatile Runnable compatProfilerClientViewHook = null;

    // Opt-in compatibility profiling. Regular /observable run and GUI profiles
    // leave this false, so AE2-specific collectors stay dormant.
    public static volatile boolean compatProfilerEnabled = false;
    // Maximum number of AE2 grids kept in full runtime detail after the short
    // scout phase. The same value also caps exported detailed grids.
    public static volatile int compatProfilerGridLimit = 0;

    public static int entityDepth = -1;
    public static int blockEntityDepth = -1;
    public static int blockDepth = -1;
    public static int fluidDepth = -1;
}
