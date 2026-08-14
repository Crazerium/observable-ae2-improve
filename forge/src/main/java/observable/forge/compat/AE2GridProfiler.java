package observable.forge.compat;

import net.minecraft.core.BlockPos;
import net.minecraft.resources.ResourceKey;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.world.level.Level;
import net.minecraft.world.level.block.entity.BlockEntity;
import net.minecraftforge.fml.loading.FMLPaths;
import observable.Observable;
import observable.Props;
import observable.server.Profiler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Per-grid AE2 profiler used by the Forge compatibility layer.
 *
 * v20.5.3 keeps the administration-first report and adds a report-side consistency guard without changing collection. Admin triage still separates measured average load from large one-off physical outliers, but now refuses confident TickManager/device attribution when Grid remainder is a dominant majority of Core or when nested inclusive buckets contradict their measured parent hierarchy. All conclusions remain derived from already collected data: no new AE2 injection points are added. AE2 timing remains explicitly opt-in with the scout Top-N runtime cap. The authoritative StorageService.ProviderState mount-owner path still covers both standard AE2 DriveBlockEntity and the exact ExtendedAE TileExDrive provider without recursive storage reflection.
 * The total physical-call distribution remains backward compatible, while a bounded per-operation breakdown separates
 * AE2 TickRateModulation outcomes and ME Drive insert/extract/preferred/available-stacks work. The dashboard also
 * reports Avg beside P50, low-sample percentile confidence, mixed/heavy-tail warnings, and conservative two-mode
 * sample splits when a large interior gap separates two sufficiently populated latency regimes.
 * Current Diagnosis and Regression Intelligence remain available; spike data is diagnostic evidence only and does
 * not attempt to optimize or change AE2 behavior.
 * Stable network identity remains duplicate-anchor-safe. Operator navigation remains:
 * device-to-grid links, per-call cost, copyable coordinates/teleport commands,
 * Top devices inside a selected Grid, dimension summaries and collapsed diagnostics. No AE2 scheduling or
 * game behavior is changed. The client receives one inclusive total marker only
 * for the requested Top-N detailed grids.
 *
 * Grid Core is inclusive. Grid Services is the sum of the service calls made by
 * Grid. Grid overhead/remainder is derived as Grid Core - Grid Services and can include both real AE2 work
 * and instrumentation overhead. Devices and the
 * TickManager sub-metrics are nested and must not be added to the inclusive
 * totals.
 */
public final class AE2GridProfiler {
    private static final Logger LOGGER = LogManager.getLogger("Observable/AE2Grid");
    private static final DateTimeFormatter REPORT_FILE_TIME = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");
    private static final int REPORT_FILE_LIMIT = 10;
    // v20.3.2.14 Runtime Top-N. The profiler briefly scouts all grids, then only the
    // requested Top-N continue through Grid lifecycle/service/queue detail. Reports remain bounded.
    private static final int DEFAULT_REPORT_GRID_LIMIT = 128;
    private static final int MAX_REPORT_GRID_LIMIT = 2048;
    private static final int REPORT_MIN_PHYSICAL_DEVICE_LIMIT = 256;
    private static final int REPORT_PHYSICAL_DEVICES_PER_GRID = 8;
    private static final int REPORT_MAX_PHYSICAL_DEVICE_LIMIT = 2048;
    private static final int REPORT_DISPATCH_LEVEL_LIMIT = 16;
    // Four server ticks are enough to rank stable production grids while keeping
    // the expensive all-grid phase short even when the server is below 20 TPS.
    private static final int RUNTIME_GRID_SCOUT_TICKS = 4;
    public static final int PHASE_SERVER_START = 0;
    public static final int PHASE_LEVEL_START = 1;
    public static final int PHASE_LEVEL_END = 2;
    public static final int PHASE_SERVER_END = 3;

    public static final int TICK_SECTION_LEVEL_QUEUE = 0;
    public static final int TICK_SECTION_QUEUE = 1;

    public static final int TICK_CONTROL_SLEEP = 0;
    public static final int TICK_CONTROL_WAKE = 1;
    public static final int TICK_CONTROL_ALERT = 2;

    public static final int TICK_BRANCH_SLEEP = 0;
    public static final int TICK_BRANCH_REQUEUE = 1;

    private static final Object LOCK = new Object();
    private static final IdentityHashMap<Object, GridInfo> GRIDS = new IdentityHashMap<>();
    private static final IdentityHashMap<Object, GridInfo> TICK_MANAGER_GRIDS = new IdentityHashMap<>();
    private static final Map<String, PhysicalDeviceRef> PHYSICAL_DEVICES = new HashMap<>();
    private static final IdentityHashMap<BlockEntity, PhysicalDeviceRef> PHYSICAL_DEVICE_HOSTS = new IdentityHashMap<>();
    // v16 dispatch registry: Level -> compact integer ID. A one-entry thread-local
    // cache makes the common TickHandler pattern (many grids for the same Level)
    // avoid a map lookup for nearly every service dispatch.
    private static final IdentityHashMap<Level, Integer> DISPATCH_LEVEL_IDS = new IdentityHashMap<>();
    private static final List<Level> DISPATCH_LEVELS = new ArrayList<>();
    private static final Set<String> RESERVED_VIRTUAL_POSITIONS = new HashSet<>();
    private static final AtomicInteger NEXT_FALLBACK_ID = new AtomicInteger(1);
    private static volatile int discoveredGridCount;

    private static final ThreadLocal<DeviceToken> DEVICE_TOKEN = new ThreadLocal<>();
    private static final ThreadLocal<ArrayDeque<TickManagerSectionToken>> TICK_MANAGER_SECTION_STACK =
            new ThreadLocal<ArrayDeque<TickManagerSectionToken>>() {
                @Override
                protected ArrayDeque<TickManagerSectionToken> initialValue() {
                    return new ArrayDeque<>();
                }
            };
    private static final ThreadLocal<ArrayDeque<TickQueueDetailToken>> TICK_QUEUE_DETAIL_STACK =
            new ThreadLocal<ArrayDeque<TickQueueDetailToken>>() {
                @Override
                protected ArrayDeque<TickQueueDetailToken> initialValue() {
                    return new ArrayDeque<>();
                }
            };
    private static final ThreadLocal<ArrayDeque<TickManagerControlToken>> TICK_MANAGER_CONTROL_STACK =
            new ThreadLocal<ArrayDeque<TickManagerControlToken>>() {
                @Override
                protected ArrayDeque<TickManagerControlToken> initialValue() {
                    return new ArrayDeque<>();
                }
            };
    private static final ThreadLocal<DispatchLevelCache> DISPATCH_LEVEL_CACHE =
            new ThreadLocal<DispatchLevelCache>() {
                @Override
                protected DispatchLevelCache initialValue() {
                    return new DispatchLevelCache();
                }
            };

    private static volatile boolean sessionActive;
    private static volatile long sessionGeneration;

    // v20.3.2.14 runtime detail selection. Identity semantics match AE2 Grid objects.
    // The selected map is built once after the scout and then only read until session end.
    private static volatile IdentityHashMap<Object, Boolean> RUNTIME_DETAILED_GRIDS = new IdentityHashMap<>();
    private static volatile boolean runtimeSelectionFrozen;
    private static volatile int runtimeScoutStartTick = Integer.MIN_VALUE;
    private static volatile int runtimeScoutTicks;
    private static volatile int runtimeDiscoveredAtFreeze;
    private static volatile int runtimeSelectedGridCount;
    private static volatile long runtimeGridLifecycleSkippedByCap;
    private static volatile long runtimeGridLifecycleKeptAfterCap;
    private static volatile long runtimeScoutGridCoreNanos;
    private static volatile long runtimeScoutServiceNanos;
    private static volatile long runtimeScoutDeviceNanos;
    private static volatile long runtimeScoutSchedulerNanos;
    // Wall-clock context is report-only and lets operators distinguish actual
    // server lag from profiler sampling counts. No hot-path reads are added.
    private static volatile long sessionStartedWallMillis;

    // v20.3.2.2 Large Server Safety. Worlds up to the historical 255-grid test size remain exact.
    // Above that threshold only hot per-Level lifecycle/service diagnostics are sampled; server-start/end
    // and physical device timings stay exact. Sampling uses rotating deterministic shards plus a hard
    // per-server-tick budget, so profiler bookkeeping cannot grow without bound with grid/level fan-out.
    private static final int LARGE_SERVER_EXACT_GRID_THRESHOLD = 256;
    private static final int LARGE_SERVER_TARGET_DETAILED_CALLBACKS_PER_TICK = 4096;
    private static final int LARGE_SERVER_HARD_DETAILED_CALLBACK_BUDGET_PER_TICK = 6144;
    private static final int LARGE_SERVER_MAX_SAMPLE_FACTOR = 64;
    private static final SamplingStats SAMPLING = new SamplingStats();
    private static final ThreadLocal<DetailScopeState> DETAIL_SCOPE =
            ThreadLocal.withInitial(DetailScopeState::new);

    private static final int FIXED_VIRTUAL_MARKERS = 17;
    private static final int MAX_SERVICE_MARKERS = 20;
    private static final int MAX_VIRTUAL_MARKERS = FIXED_VIRTUAL_MARKERS + MAX_SERVICE_MARKERS;

    // v20.3 Spike Analysis. Reservoir percentiles stay bounded even for long profiles;
    // max/top events are exact for every observed compat-wrapped physical call.
    private static final int SPIKE_RESERVOIR_SIZE = 2048;
    private static final int SPIKE_TOP_EVENTS = 8;
    private static final int SPIKE_BUCKET_TICKS = 20;
    private static final int SPIKE_MAX_BUCKETS = 180;
    // Operation breakdown is lazy and smaller than the total distribution so memory stays bounded.
    private static final int SPIKE_OPERATION_RESERVOIR_SIZE = 512;
    private static final int SPIKE_OPERATION_TOP_EVENTS = 4;
    private static final int SPIKE_MAX_OPERATION_KINDS = 12;
    // Distribution Modes are inferred at snapshot time from the already-bounded sample.
    // Each side must contain a meaningful share, and the split must have both a real adjacent gap and separated medians.
    private static final int SPIKE_MODE_MIN_ABSOLUTE_SAMPLES = 5;
    private static final double SPIKE_MODE_MIN_SHARE = 0.15;
    private static final double SPIKE_MODE_MIN_GAP_RATIO = 2.0;
    private static final double SPIKE_MODE_MIN_MEDIAN_RATIO = 4.0;

    private static final int[][] VIRTUAL_OFFSETS = {
            {0, 0},
            {2, 0}, {-2, 0}, {0, 2}, {0, -2},
            {2, 2}, {-2, 2}, {2, -2}, {-2, -2},
            {4, 0}, {-4, 0}, {0, 4}, {0, -4},
            {4, 4}, {-4, 4}, {4, -4}, {-4, -4},
            {6, 0}, {-6, 0}, {0, 6}, {0, -6}
    };

    private AE2GridProfiler() {
    }

    public static void onSessionStart() {
        synchronized (LOCK) {
            GRIDS.clear();
            TICK_MANAGER_GRIDS.clear();
            PHYSICAL_DEVICES.clear();
            PHYSICAL_DEVICE_HOSTS.clear();
            DISPATCH_LEVEL_IDS.clear();
            DISPATCH_LEVELS.clear();
            RESERVED_VIRTUAL_POSITIONS.clear();
            NEXT_FALLBACK_ID.set(1);
            discoveredGridCount = 0;
            sessionGeneration++;
            sessionActive = true;
            RUNTIME_DETAILED_GRIDS = new IdentityHashMap<>();
            runtimeSelectionFrozen = false;
            runtimeScoutStartTick = currentServerTick(null);
            runtimeScoutTicks = 0;
            runtimeDiscoveredAtFreeze = 0;
            runtimeSelectedGridCount = 0;
            runtimeGridLifecycleSkippedByCap = 0L;
            runtimeGridLifecycleKeptAfterCap = 0L;
            runtimeScoutGridCoreNanos = 0L;
            runtimeScoutServiceNanos = 0L;
            runtimeScoutDeviceNanos = 0L;
            runtimeScoutSchedulerNanos = 0L;
            sessionStartedWallMillis = System.currentTimeMillis();
            SAMPLING.reset();
        }
        DEVICE_TOKEN.remove();
        TICK_MANAGER_SECTION_STACK.remove();
        TICK_QUEUE_DETAIL_STACK.remove();
        TICK_MANAGER_CONTROL_STACK.remove();
        DISPATCH_LEVEL_CACHE.remove();
        DETAIL_SCOPE.remove();
    }

    /**
     * Registers a physical AE2 timing target for the local monitoring report.
     * This is metadata-only: timing still comes from Observable's existing
     * blockTimingsMap, so registration cannot affect AE2 execution.
     */
    static void registerPhysicalDevice(BlockEntity host, String preferredName) {
        if (!sessionActive || host == null || host.isRemoved()) {
            return;
        }
        String name = preferredName == null ? "" : preferredName;
        synchronized (LOCK) {
            PhysicalDeviceRef existing = PHYSICAL_DEVICE_HOSTS.get(host);
            if (existing != null) {
                if (!name.isBlank()) {
                    existing.preferredName = name;
                }
                return;
            }

            Level level = host.getLevel();
            if (!(level instanceof ServerLevel)) {
                return;
            }
            BlockPos pos = host.getBlockPos();
            if (pos == null) {
                return;
            }
            String dimension = level.dimension().location().toString();
            String key = physicalDeviceKey(dimension, pos);
            PhysicalDeviceRef byPosition = PHYSICAL_DEVICES.get(key);
            if (byPosition != null && byPosition.host == null) {
                byPosition.host = host;
                if (!name.isBlank()) {
                    byPosition.preferredName = name;
                }
                PHYSICAL_DEVICE_HOSTS.put(host, byPosition);
                return;
            }
            PhysicalDeviceRef ref = new PhysicalDeviceRef(
                    host, dimension, new BlockPos(pos.getX(), pos.getY(), pos.getZ()), name);
            PHYSICAL_DEVICE_HOSTS.put(host, ref);
            PHYSICAL_DEVICES.put(key, ref);
        }
    }

    /**
     * Registers a physical target by immutable dimension + coordinates when the
     * original BlockEntity has already unloaded but a stale DriveWatcher is still
     * performing storage work. This retains no level/chunk/world reference.
     */
    static void registerVirtualPhysicalDevice(ResourceKey<Level> dimension, BlockPos pos, String preferredName) {
        if (!sessionActive || dimension == null || pos == null) {
            return;
        }
        String dimensionName = dimension.location().toString();
        String key = physicalDeviceKey(dimensionName, pos);
        String name = preferredName == null ? "" : preferredName;
        synchronized (LOCK) {
            PhysicalDeviceRef existing = PHYSICAL_DEVICES.get(key);
            if (existing != null) {
                if (!name.isBlank()) {
                    existing.preferredName = name;
                }
                return;
            }
            PHYSICAL_DEVICES.put(key, new PhysicalDeviceRef(
                    null, dimensionName, new BlockPos(pos.getX(), pos.getY(), pos.getZ()), name));
        }
    }

    private static String physicalDeviceKey(String dimension, BlockPos pos) {
        return dimension + "|" + pos.getX() + "," + pos.getY() + "," + pos.getZ();
    }

    /**
     * v20.3.1: records the duration of one already-timed AE2 physical call plus an optional operation label.
     * v20.3.2.16 also exposes an opaque session-local handle so the extremely hot
     * DriveWatcher path does not repeat LOCK/map/Level lookup on every sub-microsecond call.
     */
    static Object physicalSpikeHandle(BlockEntity host) {
        if (!sessionActive || host == null) {
            return null;
        }
        synchronized (LOCK) {
            return PHYSICAL_DEVICE_HOSTS.get(host);
        }
    }

    static Object physicalSpikeHandle(ResourceKey<Level> dimension, BlockPos pos) {
        if (!sessionActive || dimension == null || pos == null) {
            return null;
        }
        synchronized (LOCK) {
            return PHYSICAL_DEVICES.get(physicalDeviceKey(dimension.location().toString(), pos));
        }
    }

    static void recordPhysicalSpike(BlockEntity host, long elapsedNanos, String operation) {
        if (host == null) {
            return;
        }
        recordPhysicalSpikeHandle(physicalSpikeHandle(host), elapsedNanos, operation);
    }

    static void recordPhysicalSpike(
            ResourceKey<Level> dimension, BlockPos pos, long elapsedNanos, String operation) {
        recordPhysicalSpikeHandle(physicalSpikeHandle(dimension, pos), elapsedNanos, operation);
    }

    /** Fast path used by session-cached DriveWatcher timing state. */
    static void recordPhysicalSpikeHandle(Object opaqueHandle, long elapsedNanos, String operation) {
        if (!sessionActive || elapsedNanos < 0L || !(opaqueHandle instanceof PhysicalDeviceRef)) {
            return;
        }
        PhysicalDeviceRef ref = (PhysicalDeviceRef) opaqueHandle;
        int tickOffset = currentProfileTickOffset();
        synchronized (ref.spikes) {
            ref.spikes.record(elapsedNanos, tickOffset, operation);
        }
    }

    private static int currentProfileTickOffset() {
        try {
            Profiler profiler = Observable.INSTANCE.getPROFILER();
            net.minecraft.server.MinecraftServer server = dev.architectury.utils.GameInstance.getServer();
            if (server != null) {
                return Math.max(0, server.getTickCount() - profiler.getStartingTicks());
            }
        } catch (Throwable ignored) {
            // Timeline metadata is optional. Distribution/max recording must still work.
        }
        return -1;
    }

    private static void linkPhysicalDeviceToGrid(BlockEntity host, GridInfo info) {
        if (host == null || info == null) {
            return;
        }
        synchronized (LOCK) {
            PhysicalDeviceRef ref = PHYSICAL_DEVICE_HOSTS.get(host);
            if (ref != null) {
                ref.gridLabel = info.label;
            }
        }
    }

    /**
     * Best-effort report-time link for passive devices such as ME Drives.
     * This only calls common AE2 node getters and never changes Grid state.
     */
    private static String resolvePhysicalDeviceGridLabel(PhysicalDeviceRef ref) {
        if (ref == null) {
            return null;
        }
        synchronized (LOCK) {
            if (ref.gridLabel != null && !ref.gridLabel.isBlank()) {
                return ref.gridLabel;
            }
        }

        Object grid = discoverGridFromPhysicalHost(ref.host);
        if (grid == null) {
            return null;
        }
        synchronized (LOCK) {
            GridInfo info = GRIDS.get(grid);
            if (info == null) {
                return null;
            }
            ref.gridLabel = info.label;
            return ref.gridLabel;
        }
    }

    private static Object discoverGridFromPhysicalHost(BlockEntity host) {
        if (host == null || host.isRemoved()) {
            return null;
        }

        Object direct = invokeNoArg(host, "getGrid");
        if (isGridObject(direct)) {
            return direct;
        }

        String[] getters = {"getMainNode", "getGridNode", "getActionableNode", "getNode", "getProxy"};
        for (String getter : getters) {
            Object carrier = invokeNoArg(host, getter);
            Object grid = gridFromNodeCarrier(carrier);
            if (grid != null) {
                return grid;
            }
        }
        return null;
    }

    private static Object gridFromNodeCarrier(Object carrier) {
        if (carrier == null) {
            return null;
        }
        if (isGridObject(carrier)) {
            return carrier;
        }
        Object grid = invokeNoArg(carrier, "getGrid");
        if (isGridObject(grid)) {
            return grid;
        }
        Object node = invokeNoArg(carrier, "getNode");
        if (node != null && node != carrier) {
            grid = findGridFromNode(node);
            if (grid != null) {
                return grid;
            }
        }
        return findGridFromNode(carrier);
    }

    /** Called by the AE2 per-device hook around IGridTickable.tickingRequest. */
    public static void beginDevice(Object node, BlockEntity host) {
        DEVICE_TOKEN.remove();
        if (Props.notProcessing || !sessionActive || node == null || !isServerProfilerThread()) {
            return;
        }

        Object grid = findGridFromNode(node);
        if (grid == null) {
            return;
        }

        GridInfo info = getOrCreateGrid(grid);
        ensureAnchor(info, host);
        linkPhysicalDeviceToGrid(host, info);
        if (runtimeSelectionFrozen && !runtimeGridSelected(grid)) {
            return;
        }
        DEVICE_TOKEN.set(new DeviceToken(info, System.nanoTime(), sessionGeneration));
    }

    public static void endDevice() {
        if (Props.notProcessing || !sessionActive) {
            DEVICE_TOKEN.remove();
            return;
        }
        endDeviceAt(System.nanoTime(), null);
    }

    static void endDeviceAt(long finishedAt, Object modulation) {
        DeviceToken token = DEVICE_TOKEN.get();
        DEVICE_TOKEN.remove();
        if (token == null || Props.notProcessing || !sessionActive || token.generation != sessionGeneration) {
            return;
        }
        synchronized (token.grid) {
            recordLocked(token.grid.devices, finishedAt - token.startedAt);
            recordModulationLocked(token.grid.tickManager, modulation);
        }
    }

    /** Begin an internal TickManagerService section such as tickLevelQueue/tickQueue. */
    public static void beginTickManagerSection(Object tickManager, int sectionId) {
        int sampleWeight = detailSampleWeight();
        if (Props.notProcessing || !sessionActive || tickManager == null || !isServerProfilerThread()
                || sampleWeight <= 0) {
            return;
        }
        TickSection section = TickSection.fromId(sectionId);
        if (section == null) {
            return;
        }
        GridInfo info = findGridForTickManager(tickManager);
        if (info == null || !runtimeGridSelected(info.grid)) {
            return;
        }
        TICK_MANAGER_SECTION_STACK.get().push(
                new TickManagerSectionToken(tickManager, info, section, sampleWeight, System.nanoTime(), sessionGeneration));
    }

    /** Finish an internal TickManagerService section. */
    public static void endTickManagerSection(Object tickManager, int sectionId) {
        TickSection section = TickSection.fromId(sectionId);
        if (tickManager == null || section == null) {
            return;
        }

        TickManagerSectionToken token = removeMatchingSectionToken(tickManager, section);
        if (token == null || Props.notProcessing || !sessionActive || token.generation != sessionGeneration) {
            return;
        }
        long finishedAt = System.nanoTime();

        synchronized (token.grid) {
            Metric metric = token.section == TickSection.LEVEL_QUEUE
                    ? token.grid.tickManager.levelQueue
                    : token.grid.tickManager.queue;
            recordLocked(metric, finishedAt - token.startedAt, token.weight);
        }
    }

    /**
     * Starts v10's fine-grained state machine for one tickQueue invocation. The
     * state machine intentionally uses only phase boundaries supplied by the
     * mixin; it never reflects into PriorityQueue/TickTracker on the hot path.
     */
    public static void beginTickQueueDetail(Object tickManager) {
        int sampleWeight = detailSampleWeight();
        if (Props.notProcessing || !sessionActive || tickManager == null || !isServerProfilerThread()
                || sampleWeight <= 0) {
            return;
        }
        GridInfo info = findGridForTickManager(tickManager);
        if (info == null || !runtimeGridSelected(info.grid)) {
            return;
        }

        // Drop an unterminated token from an exceptional previous invocation so
        // it cannot contaminate the next sample. Normal nested invocations from
        // another manager are retained.
        ArrayDeque<TickQueueDetailToken> stack = TICK_QUEUE_DETAIL_STACK.get();
        Iterator<TickQueueDetailToken> it = stack.iterator();
        while (it.hasNext()) {
            TickQueueDetailToken stale = it.next();
            if (stale.tickManager == tickManager) {
                it.remove();
                break;
            }
        }
        stack.push(new TickQueueDetailToken(tickManager, info, sampleWeight, sessionGeneration));
    }

    /** Called immediately before PriorityQueue.peek(). */
    public static void tickQueueHeadCheck(Object tickManager) {
        if (Props.notProcessing || !sessionActive) {
            return;
        }
        TickQueueDetailToken token = findTickQueueDetailToken(tickManager);
        if (!validTickQueueToken(token)) {
            return;
        }
        long now = System.nanoTime();
        closeTickQueuePhase(token, now, false);
        token.phase = TickQueuePhase.HEAD_CHECK;
        token.phaseStartedAt = now;
    }

    /** The queue head is due and PriorityQueue.poll() is about to run. */
    public static void tickQueuePoll(Object tickManager) {
        if (Props.notProcessing || !sessionActive) {
            return;
        }
        TickQueueDetailToken token = findTickQueueDetailToken(tickManager);
        if (!validTickQueueToken(token)) {
            return;
        }
        long now = System.nanoTime();
        if (token.phase == TickQueuePhase.HEAD_CHECK) {
            recordTickQueuePhase(token, TickQueuePhase.HEAD_DUE, now - token.phaseStartedAt);
        }
        token.phase = TickQueuePhase.DEQUEUE_PREP;
        token.phaseStartedAt = now;
    }

    /** unsafeTickingRequest is about to run; dequeue/diff/node preparation is complete. */
    public static void tickQueueBeforeDevice(Object tickManager) {
        if (Props.notProcessing || !sessionActive) {
            return;
        }
        TickQueueDetailToken token = findTickQueueDetailToken(tickManager);
        if (!validTickQueueToken(token)) {
            return;
        }
        long now = System.nanoTime();
        if (token.phase == TickQueuePhase.DEQUEUE_PREP) {
            recordTickQueuePhase(token, TickQueuePhase.DEQUEUE_PREP, now - token.phaseStartedAt);
        }
        // Device execution is already measured by the unsafeTickingRequest hook.
        token.phase = TickQueuePhase.NONE;
        token.phaseStartedAt = 0L;
    }

    /** setLastTick is about to run after the device callback returned. */
    public static void tickQueueAfterDevice(Object tickManager) {
        if (Props.notProcessing || !sessionActive) {
            return;
        }
        TickQueueDetailToken token = findTickQueueDetailToken(tickManager);
        if (!validTickQueueToken(token)) {
            return;
        }
        token.phase = TickQueuePhase.RATE_UPDATE;
        token.phaseStartedAt = System.nanoTime();
    }

    /** Enters either the SLEEP transition or the awake-check/requeue branch. */
    public static void tickQueueBranch(Object tickManager, int branchId) {
        if (Props.notProcessing || !sessionActive) {
            return;
        }
        TickQueueDetailToken token = findTickQueueDetailToken(tickManager);
        if (!validTickQueueToken(token)) {
            return;
        }
        long now = System.nanoTime();
        if (token.phase == TickQueuePhase.RATE_UPDATE) {
            recordTickQueuePhase(token, TickQueuePhase.RATE_UPDATE, now - token.phaseStartedAt);
        }
        token.phase = branchId == TICK_BRANCH_SLEEP
                ? TickQueuePhase.SLEEP_BRANCH
                : TickQueuePhase.AWAKE_CHECK;
        token.phaseStartedAt = now;
    }

    /** PriorityQueue.add is about to reinsert an awake tracker into the heap. */
    public static void tickQueueReinsert(Object tickManager) {
        if (Props.notProcessing || !sessionActive) {
            return;
        }
        TickQueueDetailToken token = findTickQueueDetailToken(tickManager);
        if (!validTickQueueToken(token)) {
            return;
        }
        long now = System.nanoTime();
        if (token.phase == TickQueuePhase.AWAKE_CHECK) {
            recordTickQueuePhase(token, TickQueuePhase.AWAKE_CHECK, now - token.phaseStartedAt);
        }
        token.phase = TickQueuePhase.REINSERT;
        token.phaseStartedAt = now;
    }

    /** Called from tickQueue RETURN before the inclusive tickQueue timer stops. */
    public static void finishTickQueueDetail(Object tickManager) {
        if (Props.notProcessing || !sessionActive) {
            TICK_QUEUE_DETAIL_STACK.remove();
            return;
        }
        TickQueueDetailToken token = removeTickQueueDetailToken(tickManager);
        if (!validTickQueueToken(token)) {
            return;
        }
        closeTickQueuePhase(token, System.nanoTime(), true);
    }

    private static boolean validTickQueueToken(TickQueueDetailToken token) {
        return token != null && !Props.notProcessing && sessionActive
                && token.generation == sessionGeneration && isServerProfilerThread();
    }

    private static TickQueueDetailToken findTickQueueDetailToken(Object tickManager) {
        if (tickManager == null) {
            return null;
        }
        ArrayDeque<TickQueueDetailToken> stack = TICK_QUEUE_DETAIL_STACK.get();
        if (!stack.isEmpty() && stack.peek().tickManager == tickManager) {
            return stack.peek();
        }
        for (TickQueueDetailToken token : stack) {
            if (token.tickManager == tickManager) {
                return token;
            }
        }
        return null;
    }

    private static TickQueueDetailToken removeTickQueueDetailToken(Object tickManager) {
        if (tickManager == null) {
            return null;
        }
        ArrayDeque<TickQueueDetailToken> stack = TICK_QUEUE_DETAIL_STACK.get();
        if (!stack.isEmpty() && stack.peek().tickManager == tickManager) {
            return stack.pop();
        }
        Iterator<TickQueueDetailToken> iterator = stack.iterator();
        while (iterator.hasNext()) {
            TickQueueDetailToken token = iterator.next();
            if (token.tickManager == tickManager) {
                iterator.remove();
                return token;
            }
        }
        return null;
    }

    private static void closeTickQueuePhase(TickQueueDetailToken token, long now, boolean methodReturning) {
        if (token.phase == TickQueuePhase.NONE || token.phaseStartedAt <= 0L) {
            return;
        }
        TickQueuePhase phase = token.phase;
        if (phase == TickQueuePhase.HEAD_CHECK) {
            // Only a HEAD_CHECK that reaches method RETURN without poll() is
            // the normal "next tracker is scheduled for a future tick" stop
            // path. A non-returning close should not fabricate that category.
            if (!methodReturning) {
                token.phase = TickQueuePhase.NONE;
                token.phaseStartedAt = 0L;
                return;
            }
            phase = TickQueuePhase.FUTURE_STOP;
        }
        recordTickQueuePhase(token, phase, now - token.phaseStartedAt);
        token.phase = TickQueuePhase.NONE;
        token.phaseStartedAt = 0L;
    }

    private static void recordTickQueuePhase(TickQueueDetailToken token, TickQueuePhase phase, long elapsed) {
        if (elapsed < 0L) {
            return;
        }
        Metric metric;
        TickManagerInfo tm = token.grid.tickManager;
        switch (phase) {
            case HEAD_DUE:
                metric = tm.headDueCheck;
                break;
            case DEQUEUE_PREP:
                metric = tm.dequeuePrep;
                break;
            case RATE_UPDATE:
                metric = tm.rateUpdate;
                break;
            case SLEEP_BRANCH:
                metric = tm.sleepBranch;
                break;
            case AWAKE_CHECK:
                metric = tm.awakeCheck;
                break;
            case REINSERT:
                metric = tm.reinsert;
                break;
            case FUTURE_STOP:
                metric = tm.futureStop;
                break;
            default:
                return;
        }
        synchronized (token.grid) {
            recordLocked(metric, elapsed, token.weight);
        }
    }

    /** Begin measuring ITickManager sleep/wake/alert bookkeeping. */
    public static void beginTickManagerControl(Object tickManager, int controlId) {
        int sampleWeight = detailSampleWeight();
        if (Props.notProcessing || !sessionActive || tickManager == null || !isServerProfilerThread()
                || sampleWeight <= 0) {
            return;
        }
        TickControl control = TickControl.fromId(controlId);
        if (control == null) {
            return;
        }
        GridInfo info = findGridForTickManager(tickManager);
        if (info == null || !runtimeGridSelected(info.grid)) {
            return;
        }
        TICK_MANAGER_CONTROL_STACK.get().push(
                new TickManagerControlToken(tickManager, info, control, sampleWeight, System.nanoTime(), sessionGeneration));
    }

    /** Finish measuring ITickManager sleep/wake/alert bookkeeping. */
    public static void endTickManagerControl(Object tickManager, int controlId) {
        TickControl control = TickControl.fromId(controlId);
        if (tickManager == null || control == null) {
            return;
        }

        TickManagerControlToken token = removeMatchingControlToken(tickManager, control);
        if (token == null || Props.notProcessing || !sessionActive || token.generation != sessionGeneration) {
            return;
        }
        long finishedAt = System.nanoTime();

        synchronized (token.grid) {
            recordLocked(controlMetric(token.grid.tickManager, control), finishedAt - token.startedAt, token.weight);
        }
    }

    /** Called at HEAD of one of Grid's four lifecycle dispatch methods. */
    public static Object beginGridLifecycle(Object grid, int phaseId) {
        return beginGridLifecycle(grid, phaseId, null);
    }

    /** Level-aware overload used for adaptive large-server sharding. */
    public static Object beginGridLifecycle(Object grid, int phaseId, Level level) {
        if (Props.notProcessing || !sessionActive || !isGridObject(grid) || !isServerProfilerThread()) {
            return null;
        }

        Phase phase = Phase.fromId(phaseId);
        if (phase == null) {
            return null;
        }

        int serverTick = currentServerTick(level);
        maybeFreezeRuntimeSelection(serverTick);
        if (runtimeSelectionFrozen) {
            if (!runtimeGridSelected(grid)) {
                runtimeGridLifecycleSkippedByCap++;
                return null;
            }
            runtimeGridLifecycleKeptAfterCap++;
        }

        // During the scout every grid participates. After the Top-N is frozen,
        // the sampling controller sees only the selected population, so a limit
        // such as 32 does not keep paying a 16x/32x shard chosen for thousands of grids.
        int weight = samplingWeight(grid, phase, level, serverTick, runtimeSamplingGridCount());
        if (weight <= 0) {
            return null;
        }

        GridInfo info = getOrCreateGrid(grid);
        // Anchor discovery is metadata work, not part of the timing window. On large servers it now
        // runs only for admitted samples / exact server phases instead of every skipped Level callback.
        ensureAnchor(info, null);
        return new GridLifecycleToken(grid, info, phase, weight, System.nanoTime(), sessionGeneration);
    }

    /** Called at RETURN with the opaque token returned by beginGridLifecycle. */
    public static void endGridLifecycle(Object opaqueToken) {
        if (!(opaqueToken instanceof GridLifecycleToken)) {
            return;
        }
        GridLifecycleToken token = (GridLifecycleToken) opaqueToken;
        if (Props.notProcessing || !sessionActive || token.generation != sessionGeneration) {
            return;
        }
        long elapsed = System.nanoTime() - token.startedAt;
        synchronized (token.grid) {
            recordLocked(token.grid.gridCore, elapsed, token.weight);
            recordLocked(token.grid.gridPhases.get(token.phase), elapsed, token.weight);
        }
    }

    /** Backward-compatible wrapper for older mixin overlays; new code passes the token directly. */
    public static void endGridLifecycle(Object grid, int phaseId) {
        // Intentionally no-op: v20.3.2.2's allocation-free skipped path requires token pairing.
    }

    private static int samplingWeight(Object grid, Phase phase, Level level, int serverTick, int gridCount) {
        if (phase != Phase.LEVEL_START && phase != Phase.LEVEL_END) {
            SAMPLING.recordExactLifecycle();
            return 1;
        }
        return SAMPLING.admit(grid, phase, level, serverTick, gridCount);
    }

    /** Enter the service call represented by a lifecycle sample. Null means skipped diagnostics. */
    public static void enterDetailScope(Object lifecycleToken) {
        if (Props.notProcessing || !sessionActive) {
            return;
        }
        int weight = lifecycleToken instanceof GridLifecycleToken
                ? ((GridLifecycleToken) lifecycleToken).weight : 0;
        DETAIL_SCOPE.get().push(weight);
    }

    public static void exitDetailScope() {
        if (Props.notProcessing || !sessionActive) {
            DETAIL_SCOPE.remove();
            return;
        }
        DetailScopeState state = DETAIL_SCOPE.get();
        state.pop();
        if (state.depth == 0) {
            DETAIL_SCOPE.remove();
        }
    }

    private static int detailSampleWeight() {
        DetailScopeState state = DETAIL_SCOPE.get();
        return state.depth == 0 ? 1 : state.currentWeight();
    }

    public static boolean detailedTimingSuppressed() {
        return sessionActive && detailSampleWeight() <= 0;
    }

    /**
     * Returns a stable compact ID for one loaded Level during this profile. This
     * method is only called on Observable's captured server thread. The cache is
     * intentionally checked first because AE2's level tick hook normally
     * dispatches many grids consecutively for the same ServerLevel.
     */
    private static int dispatchLevelId(Level level) {
        DispatchLevelCache cache = DISPATCH_LEVEL_CACHE.get();
        if (cache.level == level) {
            return cache.id;
        }

        Integer known = DISPATCH_LEVEL_IDS.get(level);
        int id;
        if (known != null) {
            id = known;
        } else {
            id = DISPATCH_LEVELS.size();
            DISPATCH_LEVELS.add(level);
            DISPATCH_LEVEL_IDS.put(level, id);
        }
        cache.level = level;
        cache.id = id;
        return id;
    }

    /** Begin one sampled Grid service call. Null lifecycleToken means this Level shard was skipped. */
    public static Object beginService(Object lifecycleToken, Object grid, Object service, int phaseId) {
        return beginService(lifecycleToken, grid, service, phaseId, null);
    }

    public static Object beginService(Object lifecycleToken, Object grid, Object service, int phaseId, Level level) {
        if (!(lifecycleToken instanceof GridLifecycleToken) || Props.notProcessing || !sessionActive
                || !isGridObject(grid) || service == null || !isServerProfilerThread()) {
            return null;
        }
        GridLifecycleToken lifecycle = (GridLifecycleToken) lifecycleToken;
        Phase phase = Phase.fromId(phaseId);
        if (phase == null || lifecycle.gridObject != grid || lifecycle.phase != phase
                || lifecycle.generation != sessionGeneration) {
            return null;
        }

        GridInfo info = lifecycle.grid;
        ServiceInfo serviceInfo = getOrCreateService(info, service);
        int dispatchLevelId = -1;
        if (serviceInfo.known == KnownService.TICK_MANAGER && level != null
                && (phase == Phase.LEVEL_START || phase == Phase.LEVEL_END)) {
            dispatchLevelId = dispatchLevelId(level);
        }
        return new ServiceToken(info, serviceInfo, phase, dispatchLevelId, lifecycle.weight,
                System.nanoTime(), sessionGeneration);
    }

    /** Called immediately after the service method returns (also from finally). */
    public static void endService(Object opaqueToken) {
        if (!(opaqueToken instanceof ServiceToken)) {
            return;
        }

        ServiceToken token = (ServiceToken) opaqueToken;
        if (Props.notProcessing || !sessionActive || token.generation != sessionGeneration) {
            return;
        }

        long elapsed = System.nanoTime() - token.startedAt;
        synchronized (token.grid) {
            recordLocked(token.service.total, elapsed, token.weight);
            recordLocked(token.service.phases.get(token.phase), elapsed, token.weight);

            // v16: prove where high TickManager level-dispatch fan-out comes
            // from. The Level is already reduced to a compact integer ID before
            // timing starts; no extra nanoTime or per-dispatch string allocation
            // is added on this hot path.
            if (token.dispatchLevelId >= 0
                    && (token.phase == Phase.LEVEL_START || token.phase == Phase.LEVEL_END)) {
                LevelDispatchInfo dispatch = token.grid.tickManager.dispatchFor(token.dispatchLevelId);
                recordLocked(token.phase == Phase.LEVEL_START ? dispatch.levelStart : dispatch.levelEnd, elapsed, token.weight);
            }
        }
    }

    /**
     * Builds compact diagnostic lines for the requesting player. Values are
     * microseconds per server tick, matching Observable's world overlay.
     */
    public static List<String> buildSummaryLines(int profileTicks, int limit) {
        if (profileTicks <= 0) {
            return Collections.emptyList();
        }

        List<GridSnapshot> snapshots = reportGridSnapshots(snapshotsSorted(), requestedReportGridLimit());
        int count = Math.min(Math.max(limit, 0), snapshots.size());
        List<String> lines = new ArrayList<>();

        for (int i = 0; i < count; i++) {
            GridSnapshot snapshot = snapshots.get(i);
            String label = snapshot.label;
            if (snapshot.anchor != null) {
                label += " @ " + snapshot.anchor.getX() + " " + snapshot.anchor.getY() + " " + snapshot.anchor.getZ();
            }

            StringBuilder main = new StringBuilder("[Observable/AE2] ").append(label);
            appendMetric(main, "GridCore", snapshot.gridCoreNanos, profileTicks);
            appendMetric(main, "Services", snapshot.serviceTotalNanos, profileTicks);
            appendMetric(main, "GridOnly", snapshot.gridOverheadNanos, profileTicks);
            appendMetric(main, "Devices", snapshot.deviceNanos, profileTicks);
            lines.add(main.toString());

            StringBuilder phases = new StringBuilder("  core phases:");
            int beforePhases = phases.length();
            for (Phase phase : Phase.values()) {
                MetricSnapshot metric = snapshot.gridPhases.get(phase);
                if (metric != null && metric.nanos > 0L) {
                    appendPhaseMetric(phases, phase.shortName, metric, profileTicks);
                }
            }
            if (phases.length() > beforePhases) {
                lines.add(phases.toString());
            }

            StringBuilder known = new StringBuilder("  services:");
            int beforeKnown = known.length();
            appendKnownService(known, snapshot.services, KnownService.TICK_MANAGER, "TickMgr", profileTicks);
            appendKnownService(known, snapshot.services, KnownService.STORAGE, "Storage", profileTicks);
            appendKnownService(known, snapshot.services, KnownService.CRAFTING, "Craft", profileTicks);
            appendKnownService(known, snapshot.services, KnownService.ENERGY, "Energy", profileTicks);
            appendKnownService(known, snapshot.services, KnownService.PATHING, "Path", profileTicks);
            appendKnownService(known, snapshot.services, KnownService.SPATIAL, "Spatial", profileTicks);
            appendKnownService(known, snapshot.services, KnownService.P2P, "P2P", profileTicks);
            appendKnownService(known, snapshot.services, KnownService.SECURITY, "Security", profileTicks);
            if (known.length() > beforeKnown) {
                lines.add(known.toString());
            }

            TickManagerSnapshot tickManager = snapshot.tickManager;
            if (tickManager != null && tickManager.hasInternalData()) {
                StringBuilder tickBreakdown = new StringBuilder("  tickmgr breakdown:");
                appendMetric(tickBreakdown, "Service", tickManager.serviceNanos, profileTicks);
                appendMetric(tickBreakdown, "LevelQ", tickManager.levelQueue.nanos, profileTicks);
                appendMetric(tickBreakdown, "Queue", tickManager.queue.nanos, profileTicks);
                appendMetric(tickBreakdown, "Devices", snapshot.deviceNanos, profileTicks);
                appendMetric(tickBreakdown, "QueueOnly", tickManager.queueBookkeepingNanos, profileTicks);
                appendMetric(tickBreakdown, "LevelOnly", tickManager.levelDispatchNanos, profileTicks);
                appendMetric(tickBreakdown, "Outer", tickManager.outerOverheadNanos, profileTicks);
                lines.add(tickBreakdown.toString());

                if (tickManager.hasQueuePhaseData()) {
                    StringBuilder queuePhases = new StringBuilder("  tickmgr queue phases:");
                    appendMetricWithCalls(queuePhases, "HeadDue", tickManager.headDueCheck, profileTicks);
                    appendMetricWithCalls(queuePhases, "Dequeue", tickManager.dequeuePrep, profileTicks);
                    appendMetric(queuePhases, "Devices", snapshot.deviceNanos, profileTicks);
                    appendMetricWithCalls(queuePhases, "Rate", tickManager.rateUpdate, profileTicks);
                    appendMetricWithCalls(queuePhases, "AwakeChk", tickManager.awakeCheck, profileTicks);
                    appendMetricWithCalls(queuePhases, "Reinsert", tickManager.reinsert, profileTicks);
                    appendMetricWithCalls(queuePhases, "SleepBr", tickManager.sleepBranch, profileTicks);
                    appendMetricWithCalls(queuePhases, "FutureStop", tickManager.futureStop, profileTicks);
                    appendMetric(queuePhases, "Residual", tickManager.queueResidualNanos, profileTicks);
                    lines.add(queuePhases.toString());
                }

                if (tickManager.hasControlData()) {
                    StringBuilder controls = new StringBuilder("  tickmgr controls (nested):");
                    appendMetricWithCalls(controls, "Sleep", tickManager.sleep, profileTicks);
                    appendMetricWithCalls(controls, "Wake", tickManager.wake, profileTicks);
                    appendMetricWithCalls(controls, "Alert", tickManager.alert, profileTicks);
                    lines.add(controls.toString());
                }

                if (!tickManager.modulations.isEmpty()) {
                    StringBuilder modulation = new StringBuilder("  tickmgr modulation:");
                    for (Modulation value : Modulation.values()) {
                        Long countValue = tickManager.modulations.get(value);
                        long modulationCount = countValue == null ? 0L : countValue;
                        if (modulationCount > 0L) {
                            appendCountPerTick(modulation, value.label, modulationCount, profileTicks);
                        }
                    }
                    lines.add(modulation.toString());
                }
            }

            List<ServiceSnapshot> unknown = new ArrayList<>();
            for (ServiceSnapshot service : snapshot.services) {
                if (service.known == KnownService.OTHER && service.total.nanos > 0L) {
                    unknown.add(service);
                }
            }
            if (!unknown.isEmpty()) {
                StringBuilder other = new StringBuilder("  other services:");
                int shown = 0;
                for (ServiceSnapshot service : unknown) {
                    if (shown >= 6) {
                        break;
                    }
                    appendMetric(other, service.displayName, service.total.nanos, profileTicks);
                    shown++;
                }
                if (unknown.size() > shown) {
                    other.append(" | +").append(unknown.size() - shown).append(" more");
                }
                lines.add(other.toString());
            }
        }

        if (snapshots.size() > count) {
            lines.add("[Observable/AE2] " + (snapshots.size() - count)
                    + " more grids omitted from this text summary; they remain in the detailed report.");
        }
        return lines;
    }

    public static int getGridCount() {
        synchronized (LOCK) {
            return GRIDS.size();
        }
    }

    private static int requestedReportGridLimit() {
        int requested = Props.compatProfilerGridLimit;
        if (requested <= 0) {
            requested = DEFAULT_REPORT_GRID_LIMIT;
        }
        return Math.max(1, Math.min(MAX_REPORT_GRID_LIMIT, requested));
    }

    private static int currentServerTick(Level level) {
        try {
            if (level instanceof ServerLevel) {
                return ((ServerLevel) level).getServer().getTickCount();
            }
            net.minecraft.server.MinecraftServer server = dev.architectury.utils.GameInstance.getServer();
            return server == null ? Integer.MIN_VALUE : server.getTickCount();
        } catch (Throwable ignored) {
            return Integer.MIN_VALUE;
        }
    }

    private static boolean runtimeGridSelected(Object grid) {
        if (!runtimeSelectionFrozen) {
            return true;
        }
        IdentityHashMap<Object, Boolean> selected = RUNTIME_DETAILED_GRIDS;
        return selected != null && selected.containsKey(grid);
    }

    private static void maybeFreezeRuntimeSelection(int serverTick) {
        if (runtimeSelectionFrozen || serverTick == Integer.MIN_VALUE) {
            return;
        }
        if (runtimeScoutStartTick == Integer.MIN_VALUE) {
            runtimeScoutStartTick = serverTick;
            return;
        }
        int elapsedTicks = serverTick - runtimeScoutStartTick;
        if (elapsedTicks < RUNTIME_GRID_SCOUT_TICKS) {
            return;
        }

        synchronized (LOCK) {
            if (runtimeSelectionFrozen) {
                return;
            }
            List<GridInfo> candidates = new ArrayList<>(GRIDS.values());
            candidates.sort(new Comparator<GridInfo>() {
                @Override
                public int compare(GridInfo left, GridInfo right) {
                    long leftCore;
                    long rightCore;
                    long leftDevices;
                    long rightDevices;
                    synchronized (left) {
                        leftCore = left.gridCore.nanos;
                        leftDevices = left.devices.nanos;
                    }
                    synchronized (right) {
                        rightCore = right.gridCore.nanos;
                        rightDevices = right.devices.nanos;
                    }
                    int byCore = Long.compare(rightCore, leftCore);
                    if (byCore != 0) return byCore;
                    int byDevices = Long.compare(rightDevices, leftDevices);
                    if (byDevices != 0) return byDevices;
                    return left.label.compareTo(right.label);
                }
            });

            int limit = Math.min(requestedReportGridLimit(), candidates.size());
            IdentityHashMap<Object, Boolean> selected = new IdentityHashMap<>();
            for (int i = 0; i < limit; i++) {
                selected.put(candidates.get(i).grid, Boolean.TRUE);
            }

            long scoutCore = 0L;
            long scoutServices = 0L;
            long scoutDevices = 0L;
            long scoutScheduler = 0L;
            for (GridInfo info : candidates) {
                synchronized (info) {
                    scoutCore += Math.max(0L, info.gridCore.nanos);
                    scoutDevices += Math.max(0L, info.devices.nanos);
                    long serviceTotal = 0L;
                    for (ServiceInfo service : info.services.values()) {
                        serviceTotal += Math.max(0L, service.total.nanos);
                    }
                    scoutServices += serviceTotal;
                    scoutScheduler += Math.max(0L, info.tickManager.queue.nanos - info.devices.nanos);
                }
            }

            RUNTIME_DETAILED_GRIDS = selected;
            runtimeScoutTicks = Math.max(1, elapsedTicks);
            runtimeDiscoveredAtFreeze = candidates.size();
            runtimeSelectedGridCount = selected.size();
            runtimeScoutGridCoreNanos = scoutCore;
            runtimeScoutServiceNanos = scoutServices;
            runtimeScoutDeviceNanos = scoutDevices;
            runtimeScoutSchedulerNanos = scoutScheduler;
            runtimeSelectionFrozen = true;
            LOGGER.info(
                    "Observable AE2 runtime selection frozen after {} scout ticks: {} of {} grids keep full detail",
                    runtimeScoutTicks, runtimeSelectedGridCount, runtimeDiscoveredAtFreeze);
        }
    }

    private static int runtimeSamplingGridCount() {
        if (runtimeSelectionFrozen && runtimeSelectedGridCount > 0) {
            return runtimeSelectedGridCount;
        }
        return discoveredGridCount;
    }

    private static List<GridSnapshot> reportGridSnapshots(List<GridSnapshot> allSnapshots, int limit) {
        if (allSnapshots == null || allSnapshots.isEmpty() || limit <= 0) {
            return Collections.emptyList();
        }
        if (!runtimeSelectionFrozen) {
            return firstItems(allSnapshots, limit);
        }
        List<GridSnapshot> selected = new ArrayList<>();
        for (GridSnapshot snapshot : allSnapshots) {
            if (snapshot.runtimeDetailed) {
                selected.add(snapshot);
            }
        }
        return firstItems(selected, limit);
    }

    private static int requestedPhysicalDeviceLimit(int gridLimit) {
        long scaled = (long) Math.max(1, gridLimit) * REPORT_PHYSICAL_DEVICES_PER_GRID;
        int requested = (int) Math.min(REPORT_MAX_PHYSICAL_DEVICE_LIMIT, scaled);
        return Math.max(REPORT_MIN_PHYSICAL_DEVICE_LIMIT, requested);
    }

    private static <T> List<T> firstItems(List<T> source, int limit) {
        if (source == null || source.isEmpty() || limit <= 0) {
            return Collections.emptyList();
        }
        if (source.size() <= limit) {
            return source;
        }
        return new ArrayList<>(source.subList(0, limit));
    }

    private static String snapshotDimension(GridSnapshot snapshot) {
        if (snapshot == null) {
            return "unknown";
        }
        try {
            if (snapshot.anchorEntity != null && snapshot.anchorEntity.getLevel() != null) {
                return snapshot.anchorEntity.getLevel().dimension().location().toString();
            }
        } catch (Throwable ignored) {
            // Custom Level implementations must not break report generation.
        }
        return "unknown";
    }

    /**
     * Writes a bounded AE2 grid breakdown to a standalone JSON file. This is
     * intentionally separate from Observable's own uploaded profile: the file
     * is easier to archive/compare while the normal Observable upload receives
     * only one inclusive marker for the requested Top-N grids.
     */
    public static Path writeDetailedReport(int profileTicks) {
        if (profileTicks <= 0) {
            return null;
        }

        try {
            List<GridSnapshot> allSnapshots = snapshotsSorted();
            List<PhysicalDeviceSnapshot> allPhysicalDevices = physicalDevicesSorted();
            DriveCoverageReportSnapshot driveCoverage = driveCoverageSnapshot(allPhysicalDevices);

            int gridLimit = requestedReportGridLimit();
            int physicalDeviceLimit = requestedPhysicalDeviceLimit(gridLimit);
            List<GridSnapshot> snapshots = reportGridSnapshots(allSnapshots, gridLimit);
            List<PhysicalDeviceSnapshot> physicalDevices = firstItems(allPhysicalDevices, physicalDeviceLimit);

            Path directory = FMLPaths.GAMEDIR.get().resolve("observable-reports");
            Files.createDirectories(directory);

            String baseName = "ae2-grid-" + REPORT_FILE_TIME.format(LocalDateTime.now());
            Path jsonFile = directory.resolve(baseName + ".json");
            Path htmlFile = directory.resolve(baseName + ".html");

            String json = buildDetailedReportJson(
                    allSnapshots, snapshots, allPhysicalDevices, physicalDevices,
                    driveCoverage, profileTicks, gridLimit, physicalDeviceLimit);
            Files.writeString(jsonFile, json, StandardCharsets.UTF_8);
            LOGGER.info("Observable AE2 detailed report written to {}", jsonFile.toAbsolutePath());

            try {
                Files.writeString(htmlFile, buildDetailedReportHtml(json), StandardCharsets.UTF_8);
                LOGGER.info("Observable AE2 visual report written to {}", htmlFile.toAbsolutePath());
            } catch (Throwable htmlError) {
                // The machine-readable JSON remains the primary local report.
                // A browser/reporting problem must not discard it.
                LOGGER.warn("Failed to write Observable AE2 visual HTML report", htmlError);
            }

            pruneOldDetailedReports(directory, baseName, REPORT_FILE_LIMIT);
            return jsonFile;
        } catch (Throwable t) {
            // Reporting must never make a completed profile fail.
            LOGGER.warn("Failed to write Observable AE2 detailed report", t);
            return null;
        }
    }

    /**
     * Keeps the local observable-reports directory bounded without touching unrelated files.
     * Files are grouped by ae2-grid-* basename so JSON/HTML pairs are pruned together. A partially
     * written report is treated as a one-file group. The current report is always protected.
     */
    private static void pruneOldDetailedReports(Path directory, String currentBaseName, int maxFiles) {
        if (directory == null || maxFiles <= 0) {
            return;
        }
        try {
            Map<String, ReportFileGroup> groups = new LinkedHashMap<>();
            try (Stream<Path> stream = Files.list(directory)) {
                stream.filter(Files::isRegularFile).forEach(path -> {
                    String fileName = path.getFileName().toString();
                    if (!isDetailedReportFileName(fileName)) {
                        return;
                    }
                    String baseName = reportBaseName(fileName);
                    ReportFileGroup group = groups.get(baseName);
                    if (group == null) {
                        group = new ReportFileGroup(baseName);
                        groups.put(baseName, group);
                    }
                    group.files.add(path);
                    try {
                        group.newestModifiedMillis = Math.max(group.newestModifiedMillis,
                                Files.getLastModifiedTime(path).toMillis());
                    } catch (Throwable ignored) {
                        // Name timestamp sorting below is still deterministic if mtime is unavailable.
                    }
                });
            }
            if (groups.isEmpty()) {
                return;
            }

            List<ReportFileGroup> ordered = new ArrayList<>(groups.values());
            ordered.sort((left, right) -> {
                boolean leftCurrent = left.baseName.equals(currentBaseName);
                boolean rightCurrent = right.baseName.equals(currentBaseName);
                if (leftCurrent != rightCurrent) return leftCurrent ? -1 : 1;
                int byModified = Long.compare(right.newestModifiedMillis, left.newestModifiedMillis);
                if (byModified != 0) return byModified;
                return right.baseName.compareTo(left.baseName);
            });

            int keptFiles = 0;
            int deletedFiles = 0;
            for (ReportFileGroup group : ordered) {
                boolean current = group.baseName.equals(currentBaseName);
                boolean keep = current || keptFiles + group.files.size() <= maxFiles;
                if (keep) {
                    keptFiles += group.files.size();
                    continue;
                }
                for (Path path : group.files) {
                    try {
                        if (Files.deleteIfExists(path)) {
                            deletedFiles++;
                        }
                    } catch (Throwable deleteError) {
                        LOGGER.warn("Failed to prune old Observable AE2 report {}", path.toAbsolutePath(), deleteError);
                    }
                }
            }
            if (deletedFiles > 0) {
                LOGGER.info("Observable AE2 report retention pruned {} old file(s); limit is {} ae2-grid JSON/HTML files",
                        deletedFiles, maxFiles);
            }
        } catch (Throwable retentionError) {
            // Retention is maintenance only; it must never fail a completed profile/report.
            LOGGER.warn("Failed to prune old Observable AE2 reports", retentionError);
        }
    }

    private static boolean isDetailedReportFileName(String fileName) {
        return fileName != null && fileName.startsWith("ae2-grid-")
                && (fileName.endsWith(".json") || fileName.endsWith(".html"));
    }

    private static String reportBaseName(String fileName) {
        int dot = fileName.lastIndexOf('.');
        return dot > 0 ? fileName.substring(0, dot) : fileName;
    }

    private static boolean isDiagnosticVirtualTimingName(String name) {
        return name != null && name.startsWith("AE2 Grid #");
    }

    private static List<PhysicalDeviceSnapshot> physicalDevicesSorted() {
        Map<String, PhysicalDeviceRef> registered;
        synchronized (LOCK) {
            registered = new HashMap<>(PHYSICAL_DEVICES);
        }
        if (registered.isEmpty()) {
            return Collections.emptyList();
        }

        List<PhysicalDeviceSnapshot> result = new ArrayList<>();
        Profiler profiler = Observable.INSTANCE.getPROFILER();
        for (Map.Entry<ResourceKey<Level>, ConcurrentHashMap<BlockPos, Profiler.TimingData>> dimensionEntry
                : profiler.getBlockTimingsMap().entrySet()) {
            String dimension = dimensionEntry.getKey().location().toString();
            for (Map.Entry<BlockPos, Profiler.TimingData> timingEntry : dimensionEntry.getValue().entrySet()) {
                PhysicalDeviceRef ref = registered.get(physicalDeviceKey(dimension, timingEntry.getKey()));
                if (ref == null) {
                    continue;
                }
                Profiler.TimingData data = timingEntry.getValue();
                if (data != null && isDiagnosticVirtualTimingName(data.getName())) {
                    // A virtual marker must never appear in physicalDevices. The report is
                    // normally captured before virtual publishing; keep this guard so future
                    // lifecycle/order changes cannot reintroduce logical/physical leakage.
                    continue;
                }
                String name = data == null ? null : data.getName();
                if (name == null || name.isBlank()) {
                    name = ref.preferredName;
                }
                if (name == null || name.isBlank()) {
                    name = "ae2:unknown";
                }
                long nanos = data == null ? 0L : Math.max(0L, data.getTime());
                int calls = data == null ? 0 : Math.max(0, data.getTicks());
                String gridLabel = resolvePhysicalDeviceGridLabel(ref);
                PhysicalSpikeSnapshot spike;
                synchronized (ref.spikes) {
                    spike = ref.spikes.snapshot();
                }
                result.add(new PhysicalDeviceSnapshot(
                        dimension, new BlockPos(timingEntry.getKey().getX(), timingEntry.getKey().getY(),
                        timingEntry.getKey().getZ()), name, nanos, calls, gridLabel, spike));
            }
        }

        result.sort(new Comparator<PhysicalDeviceSnapshot>() {
            @Override
            public int compare(PhysicalDeviceSnapshot left, PhysicalDeviceSnapshot right) {
                int byTime = Long.compare(right.nanos, left.nanos);
                if (byTime != 0) {
                    return byTime;
                }
                int byName = left.type.compareTo(right.type);
                if (byName != 0) {
                    return byName;
                }
                return left.dimension.compareTo(right.dimension);
            }
        });
        return result;
    }

    private static boolean isDrivePhysicalType(String type) {
        return "ae2:drive".equals(type) || "expatternprovider:ex_drive".equals(type);
    }

    private static DriveCoverageReportSnapshot driveCoverageSnapshot(List<PhysicalDeviceSnapshot> physicalDevices) {
        CompatTiming.DriveCoverageSnapshot resolver = CompatTiming.snapshotAE2DriveCoverage();
        int physicalDriveTargets = 0;
        int activeDriveTargets = 0;
        long exactTimedCalls = 0L;
        long extractCalls = 0L;
        long insertCalls = 0L;
        long preferredCalls = 0L;
        long availableStacksCalls = 0L;
        long otherCalls = 0L;

        for (PhysicalDeviceSnapshot device : physicalDevices) {
            if (!isDrivePhysicalType(device.type)) {
                continue;
            }
            physicalDriveTargets++;
            if (device.calls > 0) {
                activeDriveTargets++;
            }
            exactTimedCalls += Math.max(0, device.calls);
            if (device.spike == null || device.spike.operations == null) {
                continue;
            }
            for (OperationSpikeSnapshot operation : device.spike.operations) {
                long calls = Math.max(0L, operation.callsSeen);
                if ("drive.extract".equals(operation.name)) {
                    extractCalls += calls;
                } else if ("drive.insert".equals(operation.name)) {
                    insertCalls += calls;
                } else if ("drive.preferred".equals(operation.name)) {
                    preferredCalls += calls;
                } else if ("drive.availableStacks".equals(operation.name)) {
                    availableStacksCalls += calls;
                } else {
                    otherCalls += calls;
                }
            }
        }

        return new DriveCoverageReportSnapshot(
                resolver, physicalDriveTargets, activeDriveTargets, exactTimedCalls,
                extractCalls, insertCalls, preferredCalls, availableStacksCalls, otherCalls);
    }

    /**
     * Builds a completely self-contained HTML dashboard. No CDN, Node.js, web
     * server or network access is required: the exact report JSON is embedded
     * into the page and rendered with small inline CSS/JavaScript.
     */
    private static String buildDetailedReportHtml(String reportJson) {
        String safeJson = reportJson
                .replace("&", "\\u0026")
                .replace("<", "\\u003c")
                .replace(">", "\\u003e");

        StringBuilder html = new StringBuilder(96000);
        html.append("""
<!doctype html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Observable AE2 Monitoring v20.5.3</title>
<style>
:root{color-scheme:dark;--bg:#0b0f14;--panel:#121923;--panel2:#182230;--line:#263447;--text:#e7edf5;--muted:#93a4b8;--accent:#66d9ef;--accent2:#a6e3a1;--warn:#f9e2af;--hot:#f38ba8;--good:#94e2d5;--low:#89b4fa}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:14px/1.45 system-ui,-apple-system,Segoe UI,Roboto,sans-serif}button,input,select{font:inherit}
main{max-width:1500px;margin:0 auto;padding:24px 18px 56px}.top{display:flex;gap:18px;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;margin-bottom:12px}h1{font-size:26px;margin:0 0 5px}.muted{color:var(--muted)}
.actions,.nav-tabs{display:flex;gap:8px;flex-wrap:wrap}.actions button,.nav-tabs button,.toolbar button{appearance:none;border:1px solid var(--line);background:var(--panel2);color:var(--text);padding:8px 12px;border-radius:8px;cursor:pointer}.actions button:hover,.nav-tabs button:hover,.toolbar button:hover{border-color:#4b6688}.actions button.active,.nav-tabs button.active{border-color:var(--accent);box-shadow:0 0 0 1px var(--accent) inset;background:#122431}.nav-tabs{margin:8px 0 14px}
.note{border:1px solid #5b5130;background:#211f16;color:var(--warn);padding:10px 12px;border-radius:10px;margin:10px 0 14px}.page.hidden{display:none}
.cards{display:grid;grid-template-columns:repeat(5,minmax(150px,1fr));gap:10px;margin-bottom:12px}.card{background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:13px}.card b{display:block;font-size:22px;margin-top:3px}.card small{color:var(--muted)}
.dashboard-grid{display:grid;grid-template-columns:minmax(0,1.35fr) minmax(360px,.65fr);gap:12px}.panel,section,details.advanced{background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:13px;margin:12px 0}.panel{margin:0}.panel h2,section h2{font-size:16px;margin:0 0 10px}.panel-head{display:flex;justify-content:space-between;gap:10px;align-items:center;margin-bottom:7px}.panel-head h2{margin:0}
.toolbar{display:grid;grid-template-columns:minmax(220px,2fr) minmax(210px,1.4fr) 125px 170px 110px auto auto;gap:8px;align-items:end;background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:12px;margin-bottom:10px}.device-toolbar{grid-template-columns:minmax(250px,2fr) minmax(210px,1.4fr) 125px 150px 110px auto}.field{display:flex;flex-direction:column;gap:5px}.field label{font-size:12px;color:var(--muted)}.field input,.field select{width:100%;border:1px solid var(--line);background:#0d131b;color:var(--text);padding:8px 9px;border-radius:8px;outline:none}.field input:focus,.field select:focus{border-color:var(--accent)}.check{display:flex;align-items:center;gap:7px;height:36px;white-space:nowrap;color:var(--muted)}.check input{accent-color:var(--accent)}
.status{display:flex;gap:14px;align-items:center;flex-wrap:wrap;color:var(--muted);margin:0 0 10px}.status strong{color:var(--text)}
.table-scroll{overflow:auto;max-height:520px;border:1px solid var(--line);border-radius:9px}.table-scroll.compact{max-height:430px}.data-table{width:100%;border-collapse:collapse;min-width:850px}.data-table th,.data-table td{padding:7px 8px;border-top:1px solid rgba(38,52,71,.55);text-align:right;font-variant-numeric:tabular-nums;white-space:nowrap}.data-table th:first-child,.data-table td:first-child{text-align:left}.data-table thead{position:sticky;top:0;background:var(--panel2);z-index:2}.data-table tbody tr.clickable{cursor:pointer}.data-table tbody tr.clickable:hover{background:rgba(102,217,239,.06)}.data-table tbody tr.selected{background:rgba(102,217,239,.10)}.left{text-align:left!important}.clip{max-width:360px;overflow:hidden;text-overflow:ellipsis}.dim{max-width:330px;overflow:hidden;text-overflow:ellipsis}
.pattern-pill{display:inline-block;max-width:190px;overflow:hidden;text-overflow:ellipsis;vertical-align:middle;border:1px solid var(--line);padding:2px 7px;border-radius:999px;font-size:11px;color:var(--muted);background:#0d131b}.pattern-pill.hot{color:var(--hot);border-color:#7c3f50;background:#2a1720}.pattern-pill.warn{color:var(--warn);border-color:#665b32;background:#211f16}.pattern-pill.info{color:var(--accent);border-color:#285f6c;background:#102329}.priority-list{display:grid;gap:8px}.priority-item{display:grid;grid-template-columns:92px minmax(180px,1fr) 120px minmax(260px,2fr) auto;gap:10px;align-items:center;border-top:1px solid rgba(38,52,71,.55);padding:8px 0}.priority-item:first-child{border-top:0}.priority-rank{font-weight:700}.priority-reason{color:var(--muted)}.badge{display:inline-block;min-width:44px;text-align:center;border:1px solid var(--line);padding:2px 6px;border-radius:999px;font-size:11px}.sev-hot{color:var(--hot);border-color:#7c3f50;background:#2a1720}.sev-warn{color:var(--warn);border-color:#665b32;background:#211f16}.sev-mid{color:var(--accent);border-color:#285f6c;background:#102329}.sev-low{color:var(--low);border-color:#354d76;background:#111b2b}
.summary-wrap{display:grid;grid-template-columns:minmax(0,1.3fr) minmax(330px,.7fr);gap:12px;margin:12px 0}.selection-panel .big{font-size:28px;font-weight:700;margin:3px 0}.kv{display:grid;grid-template-columns:1fr auto;gap:5px 10px;margin-top:8px}.kv span:nth-child(odd){color:var(--muted)}
.row{display:grid;grid-template-columns:minmax(190px,1.1fr) minmax(220px,2.8fr) 115px 105px;gap:10px;align-items:center;padding:7px 0;border-top:1px solid rgba(38,52,71,.55)}.row:first-of-type{border-top:0}.label{white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.barbox{height:13px;background:#0d131b;border-radius:99px;overflow:hidden;border:1px solid #202c3a}.bar{height:100%;min-width:0;background:linear-gradient(90deg,var(--accent),var(--accent2));border-radius:99px}.value{text-align:right;font-variant-numeric:tabular-nums}.calls{text-align:right;color:var(--muted);font-variant-numeric:tabular-nums}.indent1 .label{padding-left:16px}.indent2 .label{padding-left:32px}.hot .bar{background:linear-gradient(90deg,var(--warn),var(--hot))}
.meta{display:flex;gap:15px;flex-wrap:wrap;color:var(--muted);margin:5px 0 10px}.empty{color:var(--muted);padding:8px 0}.mods{display:grid;grid-template-columns:repeat(3,minmax(150px,1fr));gap:8px}.mod{background:var(--panel2);border:1px solid var(--line);border-radius:9px;padding:10px}.mod strong{font-size:17px;display:block}
details.advanced{padding:0;overflow:hidden}details.advanced>summary{cursor:pointer;padding:13px 14px;font-weight:650;background:var(--panel2);list-style:none}details.advanced>summary::-webkit-details-marker{display:none}details.advanced>summary:before{content:'▶';display:inline-block;margin-right:8px;color:var(--accent);transition:transform .12s}details.advanced[open]>summary:before{transform:rotate(90deg)}.advanced-body{padding:2px 13px 13px}.advanced-body section{background:#0f161f;margin:10px 0}.dispatch-scroll{max-height:390px;overflow:auto;border:1px solid var(--line);border-radius:9px}.foreign{color:var(--warn)}.anchor{color:var(--good)}
.simple-hero{background:linear-gradient(135deg,#102329,#161d2a);border:1px solid #285f6c;border-radius:14px;padding:18px;margin:12px 0}.simple-hero h2{font-size:21px;margin:0 0 7px}.simple-hero .big{font-size:30px;font-weight:750;margin:4px 0}.simple-hero .lead{font-size:15px;max-width:1050px}.admin-summary{display:grid;grid-template-columns:repeat(4,minmax(150px,1fr));gap:10px;margin-bottom:12px}.admin-summary .card b{font-size:24px}.admin-findings{display:grid;gap:10px}.admin-finding{background:#0f161f;border:1px solid var(--line);border-radius:12px;padding:13px}.admin-finding.hot{border-color:#7c3f50;background:#21151c}.admin-finding.warn{border-color:#665b32;background:#1e1c15}.admin-finding.info{border-color:#285f6c}.admin-finding-head{display:flex;gap:10px;justify-content:space-between;align-items:flex-start;flex-wrap:wrap}.admin-finding h3{font-size:18px;margin:1px 0 3px}.admin-priority{font-size:12px;font-weight:750;border:1px solid var(--line);padding:3px 8px;border-radius:999px}.admin-cause{font-size:16px;font-weight:700;margin:9px 0 3px}.admin-character{color:var(--muted);margin-bottom:8px}.admin-evidence{display:flex;gap:7px;flex-wrap:wrap;margin:8px 0}.admin-evidence span{background:#0b1118;border:1px solid #202c3a;border-radius:8px;padding:5px 8px;font-variant-numeric:tabular-nums}.admin-note{color:var(--muted);max-width:1050px}.admin-meta{display:flex;gap:10px;flex-wrap:wrap;color:var(--muted);font-size:12px;margin-top:5px}.admin-spikes{display:grid;grid-template-columns:repeat(3,minmax(240px,1fr));gap:9px}.admin-spike{background:#0f161f;border:1px solid var(--line);border-radius:10px;padding:11px}.admin-spike h3{font-size:14px;margin:0 0 5px}.admin-good{border:1px solid #285f6c;background:#102329;border-radius:10px;padding:11px;color:var(--good)}.admin-guide{display:grid;grid-template-columns:repeat(3,minmax(220px,1fr));gap:8px}.admin-guide>div{background:#0f161f;border:1px solid var(--line);border-radius:9px;padding:10px}.diagnosis-grid{display:grid;grid-template-columns:repeat(2,minmax(280px,1fr));gap:9px}.diag-card{background:#0f161f;border:1px solid var(--line);border-radius:10px;padding:11px}.diag-card.hot{border-color:#7c3f50;background:#21151c}.diag-card.warn{border-color:#665b32;background:#1e1c15}.diag-card.info{border-color:#285f6c}.diag-card h3{font-size:14px;margin:0 0 5px}.diag-value{font-size:19px;font-weight:700;margin:2px 0}.diag-text{color:var(--muted);margin-top:4px}.observer-note{border-color:#3c536b;background:#101923;color:var(--muted)}.observer-note b{color:var(--text)}.spike-grid{display:grid;grid-template-columns:repeat(2,minmax(300px,1fr));gap:9px}.spike-card{background:#0f161f;border:1px solid var(--line);border-radius:10px;padding:11px}.spike-card h3{font-size:14px;margin:0 0 5px}.spike-stats{display:grid;grid-template-columns:repeat(5,minmax(76px,1fr));gap:6px;margin:8px 0}.spike-stats div{background:#0b1118;border:1px solid #202c3a;border-radius:7px;padding:6px}.spike-stats small{display:block;color:var(--muted)}.spike-shape{border:1px solid #665b32;background:#1e1c15;color:var(--warn);border-radius:7px;padding:7px 8px;margin:7px 0}.op-list{display:grid;gap:4px;margin-top:7px}.op-row{display:grid;grid-template-columns:minmax(110px,1.2fr) 70px repeat(4,minmax(70px,1fr));gap:6px;align-items:center;background:#0b1118;border:1px solid #202c3a;border-radius:7px;padding:5px 7px;font-size:12px}.op-row span:not(:first-child){text-align:right;font-variant-numeric:tabular-nums}.mode-split{margin-top:5px;padding:6px 7px;border:1px solid #285f6c;background:#101923;border-radius:7px;font-size:12px}.mode-split b{color:var(--accent)}.mode-pill{display:inline-block;margin:3px 5px 0 0;padding:2px 6px;border:1px solid #354d76;border-radius:999px;color:var(--low)}.sparkline{height:34px;display:flex;align-items:flex-end;gap:2px;margin-top:8px;padding:3px;background:#0b1118;border:1px solid #202c3a;border-radius:7px;overflow:hidden}.sparkbar{min-width:3px;flex:1;background:linear-gradient(180deg,var(--hot),var(--accent));border-radius:2px 2px 0 0}.spike-events{color:var(--muted);font-size:12px;margin-top:6px}.simple-issues{display:grid;grid-template-columns:repeat(2,minmax(300px,1fr));gap:10px}.simple-issue{background:var(--panel2);border:1px solid var(--line);border-radius:11px;padding:12px}.simple-issue.hot{border-color:#7c3f50}.simple-issue h3{font-size:15px;margin:0 0 5px}.simple-load{font-size:22px;font-weight:700}.simple-why{margin:6px 0;color:var(--muted)}.simple-actions{display:flex;gap:6px;flex-wrap:wrap;margin-top:8px}.simple-actions button{appearance:none;border:1px solid var(--line);background:#101923;color:var(--text);padding:5px 8px;border-radius:7px;cursor:pointer}.simple-actions button:hover{border-color:var(--accent)}.plain-good{color:var(--good)}.plain-warn{color:var(--warn)}.plain-hot{color:var(--hot)}.simple-list{display:grid;gap:8px}.simple-device{display:grid;grid-template-columns:minmax(210px,1.4fr) 115px minmax(210px,1.3fr) minmax(230px,1.6fr);gap:10px;align-items:center;border-top:1px solid rgba(38,52,71,.6);padding:9px 0}.simple-device:first-child{border-top:0}.simple-device .where{color:var(--muted)}.simple-compare-summary{font-size:16px;margin:8px 0 12px}.simple-shell.hidden,.expert-shell.hidden{display:none}.mode-hint{color:var(--muted);font-size:12px;margin-top:4px}.simple-explain{display:grid;grid-template-columns:repeat(3,minmax(220px,1fr));gap:8px}.simple-explain>div{background:#0f161f;border:1px solid var(--line);border-radius:9px;padding:10px}.type-grid{display:grid;grid-template-columns:repeat(3,minmax(180px,1fr));gap:8px;margin-bottom:12px}.type-card{background:var(--panel2);border:1px solid var(--line);border-radius:9px;padding:10px}.type-card strong{display:block;font-size:17px;margin:2px 0}.type-card small{color:var(--muted)}.mini-actions{display:flex;gap:5px;justify-content:flex-end;flex-wrap:wrap}.mini-btn,.grid-link{appearance:none;border:1px solid var(--line);background:#101923;color:var(--text);padding:3px 7px;border-radius:6px;cursor:pointer;font-size:11px}.mini-btn:hover,.grid-link:hover{border-color:var(--accent)}.grid-link{color:var(--accent);white-space:nowrap}.coord-line{display:flex;gap:8px;align-items:center;flex-wrap:wrap}.compare-toolbar{display:grid;grid-template-columns:minmax(230px,1.5fr) 170px 130px 130px 150px;gap:8px;align-items:end}.drop-zone{border:1px dashed #4b6688;border-radius:10px;padding:14px;background:#0d131b}.drop-zone.drag{border-color:var(--accent);background:#102329}.delta-up{color:var(--hot);font-weight:650}.delta-down{color:var(--good);font-weight:650}.delta-new{color:var(--warn);font-weight:650}.delta-gone{color:var(--muted);font-weight:650}.status-pill{display:inline-block;border:1px solid var(--line);border-radius:999px;padding:2px 7px;font-size:11px}.compare-help{display:flex;gap:14px;flex-wrap:wrap;margin-top:8px}.footer{margin-top:22px;color:var(--muted);font-size:12px}
@media(max-width:1100px){.cards,.admin-summary{grid-template-columns:repeat(2,1fr)}.toolbar,.device-toolbar,.compare-toolbar{grid-template-columns:repeat(3,minmax(150px,1fr))}.dashboard-grid,.summary-wrap{grid-template-columns:1fr}.type-grid{grid-template-columns:repeat(2,1fr)}.simple-issues,.diagnosis-grid,.spike-grid{grid-template-columns:1fr}.simple-device{grid-template-columns:1fr 110px 1fr}.simple-device .simple-actions{grid-column:1/4}.simple-explain,.admin-spikes,.admin-guide{grid-template-columns:1fr}}
@media(max-width:720px){.priority-item{grid-template-columns:1fr}.cards,.admin-summary{grid-template-columns:1fr}.toolbar,.device-toolbar,.compare-toolbar{grid-template-columns:1fr 1fr}.row{grid-template-columns:1fr 90px}.barbox{grid-column:1/3;grid-row:2}.calls{display:none}.mods,.type-grid{grid-template-columns:1fr}.data-table{min-width:760px}.simple-device{grid-template-columns:1fr}.simple-device .simple-actions{grid-column:auto}.simple-hero .big{font-size:24px}}
</style>
</head>
<body>
<main>
<div class="top"><div><h1>Observable · AE2 Monitoring</h1><div id="reportMeta" class="meta"></div></div><div class="actions"><button id="simpleModeBtn" class="active">Для администрации</button><button id="expertModeBtn">Экспертный</button><button id="jsonBtn">Открыть JSON</button><button id="copyBtn">Копировать сводку</button></div></div>
<div id="simpleShell" class="simple-shell">
 <div id="simpleHero" class="simple-hero"></div>
 <div id="simpleCards" class="admin-summary"></div>
 <section><div class="panel-head"><h2>Что требует внимания</h2><span class="muted">до 5 приоритетов · человеческий вывод из измерений</span></div><div id="adminFindings" class="admin-findings"></div></section>
 <section><div class="panel-head"><h2>Редкие события</h2><span class="muted">отдельно от постоянной нагрузки · не считать причиной TPS без повторяемости</span></div><div id="adminSpikes" class="admin-spikes"></div></section>
 <section><div class="panel-head"><h2>Как читать итог</h2><span class="muted">три правила для администрации</span></div><div class="admin-guide"><div><b>Приоритет</b><br><span class="muted">Показывает, куда смотреть в отчёте сначала. Это triage, а не автоматический вердикт о виновнике TPS.</span></div><div><b>Характер нагрузки</b><br><span class="muted">Повторяющаяся работа отделена от единичных дорогих событий, чтобы один spike не выглядел как постоянный лаг.</span></div><div><b>Уверенность</b><br><span class="muted">Высокая — причина хорошо видна в измеренных buckets. Низкая — большая часть времени остаётся в inclusive/remainder и требует экспертного разбора.</span></div></div></section>
 <details class="advanced"><summary>Сравнить с прошлым профилем</summary><div class="advanced-body"><div class="drop-zone"><b>Выбери прошлый ae2-grid-....json</b><div class="muted">Файл читается только в браузере и никуда не отправляется.</div><input id="simpleCompareFile" type="file" accept=".json,application/json" style="margin-top:10px"></div><div id="simpleCompareMeta" class="meta"></div><div id="simpleCompare"></div></div></details>
 <details class="advanced"><summary>Технические оговорки</summary><div class="advanced-body"><div class="simple-explain"><div><b>Grid Core</b><br><span class="muted">На больших серверах global Grid Core после Top-N freeze является 4-tick scout estimate. Это не полный MSPT сервера.</span></div><div><b>µs/t</b><br><span class="muted">1000 µs/t = 1 ms среднего серверного тика. Nested значения внутри одной Grid нельзя складывать.</span></div><div><b>Physical</b><br><span class="muted">Physical timing остаётся exact/full-profile. Per-Grid physical aggregate в compact отчёте — нижняя граница, если часть мелких targets omitted.</span></div></div></div></details>
</div>

<div id="expertShell" class="expert-shell hidden">
<div class="nav-tabs"><button data-view="overview" class="active">Hotspots</button><button data-view="grids">AE2 Grids</button><button data-view="devices">Physical devices</button><button data-view="compare">Compare profiles</button></div>
<div class="note">Экспертный режим: отчёт ничего не оптимизирует и не меняет в AE2. На больших серверах Level lifecycle/service diagnostics могут быть shard-sampled и масштабированы; physical device timing остаётся exact. Spike Analysis только описывает форму уже измеренных вызовов (Avg/percentiles/max/timeline + operation breakdown). Inclusive/nested значения внутри одной Grid не складываются. Compare profiles локально сравнивает отчёты. Данные никуда не отправляются.</div>

<div id="page-overview" class="page">
 <div id="globalCards" class="cards"></div>
 <section><div class="panel-head"><h2>Current diagnosis</h2><span class="muted">single-profile heuristics · no baseline required</span></div><div id="currentDiagnosis" class="diagnosis-grid"></div></section>
 <section><div class="panel-head"><h2>Diagnostic priorities</h2><span class="muted">ranked evidence from existing report data · no extra injection points</span></div><div id="diagnosticPriorities" class="priority-list"></div></section>
 <section><div class="panel-head"><h2>Spike Analysis</h2><span class="muted">sample-derived Distribution Modes + operation-aware distributions; no new AE2 injection points</span></div><div id="spikeSummary" class="spike-grid"></div></section>
 <div class="dashboard-grid">
  <div class="panel"><div class="panel-head"><h2>Top AE2 Grids</h2><span class="muted">клик → открыть Grid</span></div><div id="topGrids"></div></div>
  <div class="panel"><div class="panel-head"><h2>Dimensions</h2><span class="muted">Top-N detail · global total above</span></div><div id="dimensionSummary"></div></div>
 </div>
 <section><div class="panel-head"><h2>Top physical AE2 devices</h2><span class="muted">отдельные timing buckets; не прибавлять к Grid Core</span></div><div id="topDevices"></div></section>
  <div class="dashboard-grid">
   <div class="panel"><div class="panel-head"><h2>Physical load by Grid</h2><span class="muted">exported targets · conservative lower bound when compact</span></div><div id="physicalByGrid"></div></div>
   <div class="panel"><div class="panel-head"><h2>Device types</h2><span class="muted">Σ load / count / avg call</span></div><div id="hotspotTypes"></div></div>
  </div>
</div>

<div id="page-grids" class="page hidden">
 <div class="toolbar">
  <div class="field"><label for="gridSearch">Поиск Grid / dimension / координаты</label><input id="gridSearch" type="search" placeholder="например: #82, overworld, ps_adobeaudition, -23 64 6"></div>
  <div class="field"><label for="gridDimension">Dimension</label><select id="gridDimension"></select></div>
  <div class="field"><label for="gridMin">Минимум Core, µs/t</label><input id="gridMin" type="number" min="0" step="0.1" value="5"></div>
  <div class="field"><label for="gridSort">Сортировка</label><select id="gridSort"><option value="priority">Диагностический приоритет</option><option value="core">Grid Core</option><option value="devices">Devices</option><option value="scheduler">Scheduler</option><option value="overhead">Grid remainder*</option><option value="services">Services</option><option value="dispatch">Level dispatch</option><option value="foreign">Foreign dispatch</option></select></div>
  <div class="field"><label for="gridTop">Показывать</label><select id="gridTop"><option value="10">Top 10</option><option value="25">Top 25</option><option value="50" selected>Top 50</option><option value="100">Top 100</option><option value="0">Все</option></select></div>
  <label class="check"><input id="gridActive" type="checkbox"> Только с Devices</label>
  <button id="gridShowAll">Показать все</button>
 </div>
 <div id="gridStatus" class="status"></div>
 <div id="gridList"></div>
 <div class="summary-wrap"><div class="panel"><h2>Выбранная сеть</h2><div id="selectionSummary"></div></div><div class="panel"><h2>Быстрый смысл</h2><div class="muted">Обычный мониторинг оставляет здесь только основные метрики. Внутренние lifecycle/queue/dispatch счётчики спрятаны ниже в <b>Advanced diagnostics</b>.</div></div></div>
 <div id="gridDetail"></div>
</div>

<div id="page-devices" class="page hidden">
 <div class="toolbar device-toolbar">
  <div class="field"><label for="deviceSearch">Поиск type / dimension / координаты</label><input id="deviceSearch" type="search" placeholder="например: ae2:drive, Grid #50, ps_adobeaudition, 10 64 -3"></div>
  <div class="field"><label for="deviceDimension">Dimension</label><select id="deviceDimension"></select></div>
  <div class="field"><label for="deviceMin">Минимум, µs/t</label><input id="deviceMin" type="number" min="0" step="0.1" value="0"></div>
  <div class="field"><label for="deviceSort">Сортировка</label><select id="deviceSort"><option value="priority">Диагностический приоритет</option><option value="time">Нагрузка</option><option value="avg">Avg / call</option><option value="calls">Calls/t</option><option value="p99">P99 / call</option><option value="max">Max / call</option><option value="type">Тип</option></select></div>
  <div class="field"><label for="deviceTop">Показывать</label><select id="deviceTop"><option value="25">Top 25</option><option value="50" selected>Top 50</option><option value="100">Top 100</option><option value="0">Все</option></select></div>
  <button id="deviceShowAll">Показать все</button>
 </div>
 <div id="deviceStatus" class="status"></div>
 <div id="typeSummary"></div>
 <div id="deviceList"></div>
</div>

<div id="page-compare" class="page hidden">
 <section>
  <div class="panel-head"><h2>Compare profiles</h2><span class="muted">текущий HTML = Current, выбранный JSON = Baseline</span></div>
  <div id="compareDrop" class="drop-zone"><div><b>Выбери предыдущий ae2-grid-....json</b></div><div class="muted">Файл читается только локально браузером. Можно выбрать кнопкой или перетащить сюда.</div><input id="compareFile" type="file" accept=".json,application/json" style="margin-top:10px"></div>
  <div id="compareMeta" class="meta"></div>
 </section>
 <div class="compare-toolbar panel">
  <div class="field"><label for="compareKind">Показывать</label><select id="compareKind"><option value="all">Все изменения</option><option value="regressions">Только регрессии</option><option value="improvements">Только улучшения</option><option value="new">Только новые</option><option value="gone">Только исчезнувшие</option></select></div>
  <div class="field"><label for="compareSort">Сортировка</label><select id="compareSort"><option value="delta">Δ нагрузки</option><option value="pct">% изменения</option><option value="current">Current</option><option value="baseline">Baseline</option></select></div>
  <div class="field"><label for="comparePct">Мин. изменение, %</label><input id="comparePct" type="number" min="0" step="1" value="10"></div>
  <div class="field"><label for="compareAbs">Мин. |Δ|, µs/t</label><input id="compareAbs" type="number" min="0" step="0.1" value="0"></div>
  <div class="field"><label for="compareTop">Показывать</label><select id="compareTop"><option value="25">Top 25</option><option value="50">Top 50</option><option value="100" selected>Top 100</option><option value="0">Все</option></select></div>
 </div>
 <div id="compareCards" class="cards"></div>
 <section><div class="panel-head"><h2>Regression intelligence</h2><span class="muted">calls/tick vs cost/call + topology diff; correlation ≠ causation</span></div><div id="compareInsights"><div class="empty">Выбери baseline JSON.</div></div></section>
 <section><div class="panel-head"><h2>AE2 Grid changes</h2><span class="muted">runtime Top-N detail · Grid totals may be scout estimate</span></div><div id="compareGridList"><div class="empty">Выбери baseline JSON.</div></div></section>
 <section><div class="panel-head"><h2>Physical device changes</h2><span class="muted">match: dimension + type + coordinates</span></div><div id="compareDeviceList"><div class="empty">Выбери baseline JSON.</div></div></section>
</div>
</div>

""");
        html.append("""
<div class="footer">Observable AE2 v20.5.3 Administration Consistency Guard + v20.5.2 Administration Accuracy + human-readable triage + retained v20.4.1 Diagnostic Intelligence + Physical Grid Aggregate + Secondary Foreign Dispatch + Robust Burst Detection + v20.3.2.16 Drive Hot Path Cache + ExtendedAE Drive Mount Ownership + Runtime Top-N + Compact Reports + Large Server Safety + Profiler Correctness + Distribution Modes + Regression Intelligence. Regular /observable run does not collect AE2 detail. Use /observable ae2 run &lt;seconds&gt; or /observable ae2 &lt;grids&gt; run &lt;seconds&gt;. After a 4-tick scout only Top-N grids keep full runtime detail; Grid global totals are scout-estimated when the cap freezes, while physical totals remain full-profile. Moderator TP uses /observable tp.</div>
</main>
<script id="ae2-data" type="application/json">
__REPORT_JSON__
</script>
""");
        html.append("""
<script>
const report=JSON.parse(document.getElementById('ae2-data').textContent);
const allGrids=[...(report.grids||[])];
const allDevices=[...(report.physicalDevices||[])];
const reportLimits=report.reportLimits||{};
const globalTotals=report.globalTotals||{};
const runtimeSelection=report.runtimeSelection||{};
const n=v=>Number.isFinite(Number(v))?Number(v):0;
const globalValue=(key,fallback)=>Number.isFinite(Number(globalTotals?.[key]))?n(globalTotals[key]):fallback;
const observedPhysicalCount=()=>globalValue('observedPhysicalDevices',allDevices.length);
const globalPhysicalTotal=()=>globalValue('physicalUsPerTick',allDevices.reduce((a,d)=>a+n(d.metric?.usPerTick),0));
const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const fmt=v=>{v=n(v);return `${v>=100?v.toFixed(1):v>=10?v.toFixed(2):v.toFixed(3)} µs/t`};
const calls=v=>`${n(v)>=10?n(v).toFixed(1):n(v).toFixed(3)}x/t`;
const avgCallUs=m=>n(m?.usPerCall)||(n(m?.calls)>0?n(m?.totalNanos)/1000/n(m?.calls):0);
const fmtCall=v=>{v=n(v);return v>=1000?`${(v/1000).toFixed(v>=10000?1:2)} ms/call`:`${v>=100?v.toFixed(1):v>=10?v.toFixed(2):v.toFixed(3)} µs/call`};
const spikeOf=d=>d?.spike||{};
const spikeVal=(d,k)=>n(spikeOf(d)?.[k]);
const spikeOps=d=>Array.isArray(spikeOf(d)?.operations)?spikeOf(d).operations:[];
function spikeConfidence(s){const seen=n(s?.callsSeen),captured=n(s?.samplesCaptured),sampled=!!s?.reservoirSampled;let label='good sample';if(seen<20)label='very low sample';else if(seen<100)label='P99 low-sample';else if(seen<500)label='moderate sample';return `${sampled?`reservoir ${captured.toLocaleString()}/${seen.toLocaleString()}`:`exact n=${captured.toLocaleString()}`} · ${label}`}
const spikeSampleNote=d=>spikeConfidence(spikeOf(d));
function spikeModes(s){return Array.isArray(s?.modes)?s.modes:[]}
function spikeModeSummary(s){const ms=spikeModes(s);if(ms.length<2)return '';const a=ms[0],b=ms[1],sep=n(s?.modeMedianRatio);return `Sample-derived modes: ${n(a.sampleSharePct).toFixed(0)}% @ P50 ${fmtCall(a.p50Us)} vs ${n(b.sampleSharePct).toFixed(0)}% @ P50 ${fmtCall(b.p50Us)} · median separation ×${sep.toFixed(1)}`}
function spikeModesHtml(s){const ms=spikeModes(s);if(ms.length<2)return '';const sample=!!s?.reservoirSampled?'reservoir sample':'exact sample';return `<div class="mode-split"><b>Distribution Modes</b> · ${esc(sample)} · separation ×${n(s?.modeMedianRatio).toFixed(1)}<br>${ms.map((m,i)=>`<span class="mode-pill">${i?'Slow':'Fast'} ~${n(m.sampleSharePct).toFixed(0)}% · n=${n(m.sampleCount)} · P50 ${fmtCall(m.p50Us)} · P95 ${fmtCall(m.p95Us)}</span>`).join('')}</div>`}
function spikeShape(s,avg){const modes=spikeModes(s);if(modes.length>=2)return `Two latency modes detected in the captured sample. ${spikeModeSummary(s)}. Это статистическое разделение, не доказательство конкретной ветки AE2.`;const seen=n(s?.callsSeen),p50=n(s?.p50Us),p95=n(s?.p95Us),p99=n(s?.p99Us);if(seen<8||p50<=0)return null;const tail=p95/p50,tail99=p99/p50,meanRatio=n(avg)/p50;if(tail>=5&&(meanRatio>=2.5||tail99>=8))return `Mixed/heavy-tail shape: Avg/P50 ×${meanRatio.toFixed(1)}, P95/P50 ×${tail.toFixed(1)}. Одна медиана не описывает такой target.`;return null}
function spikeTimelineHtml(d){const tl=spikeOf(d)?.timeline||[];if(!tl.length)return '<div class="muted">Timeline недоступен для этого target.</div>';const max=Math.max(1,...tl.map(x=>n(x.maxUs)));return `<div class="sparkline" title="каждый столбец = 20 server ticks; высота = max call">${tl.map(x=>`<span class="sparkbar" style="height:${Math.max(3,Math.min(100,n(x.maxUs)/max*100))}%" title="tick ${n(x.startTick)}-${n(x.startTick)+19}: ${n(x.calls)} calls, max ${fmtCall(n(x.maxUs))}"></span>`).join('')}</div>`}
function spikeEventsText(d){const ev=spikeOf(d)?.topEvents||[];return ev.length?ev.slice(0,5).map(x=>`t+${n(x.tick)}: ${fmtCall(n(x.us))}`).join(' · '):'нет captured spike events'}
function operationLabel(name){name=String(name||'unknown');if(name.startsWith('drive.'))return `Drive ${name.slice(6)}`;if(name==='tick')return 'AE2 tick';if(name==='unknown'||name==='other')return name;return `Tick modulation ${name}`}
function spikeOperationHtml(d){const ops=spikeOps(d);if(!ops.length)return '';const rows=ops.slice(0,8).map(o=>`<div><div class="op-row"><b>${esc(operationLabel(o.name))}</b><span>n=${n(o.callsSeen).toLocaleString()}</span><span title="Avg">${fmtCall(o.avgUs)}</span><span title="P50">${fmtCall(o.p50Us)}</span><span title="P95">${fmtCall(o.p95Us)}</span><span title="Max">${fmtCall(o.maxUs)}</span></div>${spikeModesHtml(o)}</div>`).join('');const more=ops.length>8?`<div class="mode-hint">+${ops.length-8} operation groups</div>`:'';return `<div class="mode-hint">Operation breakdown: Avg · P50 · P95 · Max</div><div class="op-list">${rows}</div>${more}`}
function spikeCardHtml(d){const s=spikeOf(d),avg=avgCallUs(d.metric),shape=spikeShape(s,avg);return `<div class="spike-card"><h3>${esc(humanType(d.type))} · ${esc(dimShort(d.dimension))} @ ${esc(posText(d.position))}</h3><div class="spike-stats"><div><small>Avg</small><b>${fmtCall(avg)}</b></div><div><small>P50</small><b>${fmtCall(s.p50Us)}</b></div><div><small>P95</small><b>${fmtCall(s.p95Us)}</b></div><div><small>P99</small><b>${fmtCall(s.p99Us)}</b></div><div><small>Exact max</small><b>${fmtCall(s.maxUs)}</b></div></div>${shape?`<div class="spike-shape">${esc(shape)}</div>`:''}${spikeModesHtml(s)}${spikeTimelineHtml(d)}<div class="spike-events">${esc(spikeEventsText(d))}</div><div class="mode-hint">${esc(spikeSampleNote(d))}${s.timelineTruncated?' · timeline truncated':''}${s.operationOverflow?' · operation groups capped':''}</div>${spikeOperationHtml(d)}<div class="simple-actions">${coordActions(d.dimension,d.position)}${d.gridLabel&&gridByLabel(d.gridLabel)?`<button data-open-grid="${esc(d.gridLabel)}">Открыть сеть</button>`:''}</div></div>`}
function spikeCandidates(limit=6){return allDevices.filter(d=>n(spikeOf(d)?.callsSeen)>0).sort((a,b)=>spikeVal(b,'maxUs')-spikeVal(a,'maxUs')||spikeVal(b,'p99Us')-spikeVal(a,'p99Us')).slice(0,limit)}
function renderSpikeAnalysis(){const ds=spikeCandidates(6),html=ds.length?ds.map(spikeCardHtml).join(''):'<div class="empty">В этом профиле ещё нет captured physical-call spike samples.</div>';const a=document.getElementById('simpleSpikes'),b=document.getElementById('spikeSummary');if(a)a.innerHTML=html;if(b)b.innerHTML=html}
const dimShort=d=>{d=String(d||'unknown');const p=d.lastIndexOf('/');return p>=0?d.slice(p+1):d};
const posText=p=>p?`${p.x} ${p.y} ${p.z}`:'';
const anchorText=g=>posText(g?.anchor);
const gridByLabel=label=>allGrids.find(g=>g.label===label)||null;
const tpCommand=(dimension,p)=>p?`/observable tp ${dimension} position ${n(p.x)} ${n(p.y)} ${n(p.z)}`:'';
const coordActions=(dimension,p)=>p?`<span class="mini-actions"><button class="mini-btn" data-copy-text="${esc(posText(p))}">Copy coords</button><button class="mini-btn" data-copy-text="${esc(tpCommand(dimension,p))}">Copy TP</button></span>`:'';
const gridLink=label=>label?(gridByLabel(label)?`<button class="grid-link" data-open-grid="${esc(label)}">${esc(label)}</button>`:`<span class="muted" title="Grid has physical targets but was not exported by runtime Top-N detail">${esc(label)} · physical-only</span>`):'<span class="muted">unlinked</span>';
const dispatchTotal=g=>Number.isFinite(Number(g?.tickManager?.dispatchLevelsAggregate?.usPerTick))?n(g.tickManager.dispatchLevelsAggregate.usPerTick):(g?.tickManager?.dispatchLevels||[]).reduce((a,x)=>a+n(x.total?.usPerTick),0);
const foreignDispatch=g=>Number.isFinite(Number(g?.tickManager?.foreignDispatchAggregate?.usPerTick))?n(g.tickManager.foreignDispatchAggregate.usPerTick):(g?.tickManager?.dispatchLevels||[]).filter(x=>!x.anchorDimension).reduce((a,x)=>a+n(x.total?.usPerTick),0);
const valueOf=(g,kind)=>{if(!g)return 0;switch(kind){case'devices':return n(g.metrics?.devices?.usPerTick);case'scheduler':return n(g.tickManager?.schedulerRemainder?.usPerTick);case'overhead':return n(g.metrics?.gridOverhead?.usPerTick);case'services':return n(g.metrics?.gridServices?.usPerTick);case'dispatch':return dispatchTotal(g);case'foreign':return foreignDispatch(g);default:return n(g.metrics?.gridCore?.usPerTick)}};
const severity=v=>v>=100?['HOT','sev-hot']:v>=25?['HIGH','sev-warn']:v>=5?['MID','sev-mid']:['LOW','sev-low'];
const badge=v=>{const [t,c]=severity(n(v));return `<span class="badge ${c}">${t}</span>`};
const metric=(label,m,max,indent=0,hot=false)=>{const v=n(m?.usPerTick),pct=Math.min(100,max>0?v/max*100:0);return `<div class="row indent${indent}${hot?' hot':''}"><div class="label" title="${esc(label)}">${esc(label)}</div><div class="barbox"><div class="bar" style="width:${pct.toFixed(2)}%"></div></div><div class="value">${fmt(v)}</div><div class="calls">${calls(m?.callsPerTick)}</div></div>`};
const section=(title,body)=>`<section><h2>${esc(title)}</h2>${body||'<div class="empty">Нет данных</div>'}</section>`;
let siteMode='simple';
let currentView='overview';
let selectedGrid=null;
let gridFiltered=[];
let deviceFiltered=[];
const gridState={q:'',dimension:'*',min:5,sort:'core',top:50,active:false};
const deviceState={q:'',dimension:'*',min:0,sort:'time',top:50};
let baselineReport=null;let baselineName='';
const compareState={kind:'all',sort:'delta',minPct:10,minAbs:0,top:100};
const UI_STORAGE_KEY='observable-ae2-v20.5.3-ui';

function switchView(view,persist=true){currentView=view;document.querySelectorAll('.page').forEach(p=>p.classList.toggle('hidden',p.id!==`page-${view}`));document.querySelectorAll('.nav-tabs button').forEach(b=>b.classList.toggle('active',b.dataset.view===view));if(view==='overview')renderOverview();if(view==='grids')applyGridFilters(false);if(view==='devices')applyDeviceFilters(false);if(view==='compare')renderCompare();if(persist)saveUiState()}
function setSiteMode(mode,persist=true){siteMode=mode==='expert'?'expert':'simple';document.getElementById('simpleShell').classList.toggle('hidden',siteMode!=='simple');document.getElementById('expertShell').classList.toggle('hidden',siteMode!=='expert');document.getElementById('simpleModeBtn').classList.toggle('active',siteMode==='simple');document.getElementById('expertModeBtn').classList.toggle('active',siteMode==='expert');if(siteMode==='simple')renderSimple();else switchView(currentView,false);if(persist)saveUiState()}
function populateDimensionSelect(id,values){const el=document.getElementById(id);el.innerHTML='<option value="*">Все dimensions</option>'+values.map(d=>`<option value="${esc(d)}">${esc(dimShort(d))}</option>`).join('')}
const pctText=(v,total)=>total>0?`${(n(v)/total*100).toFixed(n(v)/total*100>=10?1:2)}%`:'—';
const gridKey=g=>`${g?.dimension||'unknown'}|${n(g?.anchor?.x)},${n(g?.anchor?.y)},${n(g?.anchor?.z)}`;
const deviceKey=d=>`${d?.dimension||'unknown'}|${d?.type||'unknown'}|${n(d?.position?.x)},${n(d?.position?.y)},${n(d?.position?.z)}`;
const HUMAN_TYPES={'ae2:export_bus':'Шина экспорта AE2','ae2:import_bus':'Шина импорта AE2','ae2:storage_bus':'Шина хранения AE2','ae2:drive':'ME Drive','ae2:charger':'Зарядник AE2','ae2:dense_energy_cell':'Плотная энергетическая ячейка','ae2:energy_cell':'Энергетическая ячейка','expatternprovider:tag_export_bus':'Tag Export Bus','expatternprovider:ex_drive':'ExtendedAE Drive','expatternprovider:ex_export_bus_part':'Extended Export Bus','expatternprovider:ex_import_bus_part':'Extended Import Bus','expatternprovider:oversize_interface':'Oversize Interface'};
const humanType=t=>HUMAN_TYPES[t]||String(t||'unknown').replace(/^ae2:/,'AE2 ').replace(/^expatternprovider:/,'ExtendedAE ').replace(/_/g,' ');
const ms=v=>`${(n(v)/1000).toFixed(n(v)>=10000?1:2)} ms/t`;
const tickBudgetPct=v=>n(v)/50000*100;
function simpleGrade(v){const p=tickBudgetPct(v);return p>=10?['Высокая нагрузка','plain-hot']:p>=4?['Заметная нагрузка','plain-warn']:['Умеренная нагрузка','plain-good']}
function physicalForGrid(label,devices=allDevices){return devices.filter(d=>d.gridLabel===label).sort((a,b)=>n(b.metric?.usPerTick)-n(a.metric?.usPerTick))}
function deviceFrequencyText(d){const c=n(d?.metric?.callsPerTick),avg=avgCallUs(d?.metric);if(c<=0)return 'За этот профиль не срабатывало.';if(c<0.05&&avg>=1000)return `Срабатывает редко, но один вызов тяжёлый: ${fmtCall(avg)}.`;if(c>=0.8)return `Работает почти каждый тик; одно срабатывание ${fmtCall(avg)}.`;if(c<0.1)return `Срабатывает примерно раз в ${Math.max(1,Math.round(1/c))} тиков; одно срабатывание ${fmtCall(avg)}.`;return `Срабатывает ${calls(c)}; одно срабатывание ${fmtCall(avg)}.`}
function dispatchStats(g){const tm=g?.tickManager||{},levels=tm.dispatchLevels||[],anchor=levels.filter(x=>x.anchorDimension),foreign=levels.filter(x=>!x.anchorDimension),sum=arr=>arr.reduce((a,x)=>a+n(x.total?.usPerTick),0),totalLevels=Number.isFinite(Number(tm.dispatchLevelsTotal))?n(tm.dispatchLevelsTotal):levels.length;return{levels:totalLevels,shown:levels.length,omitted:Math.max(0,totalLevels-levels.length),anchorCount:anchor.length,foreignCount:foreign.length,anchorLoad:Number.isFinite(Number(tm.dispatchLevelsAggregate?.usPerTick))?Math.max(0,n(tm.dispatchLevelsAggregate.usPerTick)-n(tm.foreignDispatchAggregate?.usPerTick)):sum(anchor),foreignLoad:Number.isFinite(Number(tm.foreignDispatchAggregate?.usPerTick))?n(tm.foreignDispatchAggregate.usPerTick):sum(foreign)}}
function physicalConcentration(g,topN=5){const active=physicalForGrid(g?.label).filter(d=>n(d.metric?.usPerTick)>0),physicalTotal=active.reduce((a,d)=>a+n(d.metric?.usPerTick),0),top=active.slice(0,topN),topTotal=top.reduce((a,d)=>a+n(d.metric?.usPerTick),0),devices=valueOf(g,'devices');return{activeCount:active.length,physicalTotal,top,topCount:top.length,topTotal,physicalShare:physicalTotal>0?topTotal/physicalTotal*100:0,devicesShare:devices>0?topTotal/devices*100:0}}
function observerRisk(g){const callsPerTick=n(g?.metrics?.gridCore?.callsPerTick),remainder=valueOf(g,'overhead'),core=valueOf(g,'core'),ratio=core>0?remainder/core*100:0;let level='low';if(callsPerTick>=80&&remainder>=25)level='elevated';else if(callsPerTick>=40&&remainder>=10)level='possible';return{level,callsPerTick,remainder,ratio}}
function observerRiskText(r){return r.level==='elevated'?'повышенный':r.level==='possible'?'возможный':'низкий'}
function observerRiskClass(r){return r.level==='elevated'?'plain-warn':r.level==='possible'?'plain-warn':'plain-good'}
function simpleGridCause(g){const gp=gridPattern(g),pd=physicalForGrid(g.label)[0],conc=physicalConcentration(g);let text=`${gp.label}: ${gp.reason}`;if(conc.activeCount>=5&&conc.physicalShare>=70&&conc.physicalTotal>=1)text+=` Top ${conc.topCount} physical targets дают ${conc.physicalShare.toFixed(1)}% их измеренной physical load.`;if(pd&&n(pd.metric?.usPerTick)>=1){const dp=devicePattern(pd);text+=` Самый заметный physical target: ${humanType(pd.type)} (${fmt(pd.metric?.usPerTick)}; ${dp.label.toLowerCase()}).`}return text}
function devicePattern(d){
 const load=n(d?.metric?.usPerTick),c=n(d?.metric?.callsPerTick),metricCalls=n(d?.metric?.calls),avg=avgCallUs(d?.metric),totalUs=n(d?.metric?.totalNanos)/1000,sp=spikeOf(d),seen=n(sp?.callsSeen),p50=n(sp?.p50Us),p95=n(sp?.p95Us),p99=n(sp?.p99Us),max=n(sp?.maxUs),modes=sp?.modes||[];
 const maxShare=totalUs>0?Math.min(1,max/totalUs):0;
 const rareBurst=max>=1000&&seen>0&&(seen<=8||c<0.15);
 const largeOutlier=max>=1000&&seen>0&&(maxShare>=0.20||(p99>0&&max>=Math.max(5000,p99*20)));
 const typicalAvg=largeOutlier&&metricCalls>1?Math.max(0,totalUs-max)/Math.max(1,metricCalls-1):avg;
 let key='mixed',label='Смешанная',level='info',confidence='medium',reason=`${fmt(load)} · ${calls(c)} · Avg ${fmtCall(avg)}`;
 if(c>=100&&typicalAvg<1){key='throughput';label='Высокий поток дешёвых calls';level=load>=100?'warn':'info';confidence='high';reason=largeOutlier?`${calls(c)}, typical без top max ≈ ${fmtCall(typicalAvg)} (Avg с outlier ${fmtCall(avg)}); основная работа — большой поток дешёвых вызовов.`:`${calls(c)}, Avg ${fmtCall(avg)}; нагрузка создаётся объёмом вызовов, а не ценой одного.`}
 if(key!=='throughput'&&c>=0.5&&load>=10&&typicalAvg>=1){key='sustained';label='Постоянная нагрузка';level=load>=100?'hot':'warn';confidence='high';reason=`${calls(c)}, typical ${fmtCall(typicalAvg)}; вклад повторяется регулярно.`}
 if(c>0&&c<0.25&&avg>=250){key='rare-costly';label='Редкий дорогой call';level=avg>=1000?'warn':'info';confidence='high';reason=`${calls(c)}, Avg ${fmtCall(avg)}; средний µs/t невысок из-за редкой частоты.`}
 if(rareBurst){key='burst';label='Редкий spike';level=max>=5000?'hot':'warn';confidence='high';reason=`${seen.toLocaleString()} calls за профиль (${calls(c)}), max ${fmtCall(max)}; средняя нагрузка ${fmt(load)} скрывает редкий stall.`}
 if(key!=='burst'&&modes.length>=2){key='bimodal';label='Два режима latency';level='warn';confidence='medium';reason=`Распределение разделилось на ${modes.length} режима; Avg ${fmtCall(avg)}, P99 ${fmtCall(p99)}.`}
 else if(key!=='burst'&&p50>0&&p99>=Math.max(50,p50*8)&&c>=0.2){key='tail';label='Тяжёлый хвост';level=p99>=1000?'warn':'info';confidence='medium';reason=`P50 ${fmtCall(p50)} → P99 ${fmtCall(p99)}; редкие calls заметно дороже нормы.`}
 if(largeOutlier&&key!=='burst'){
  label=`${label} + крупный outlier`;
  if(max>=5000)level='hot';else if(level==='info')level='warn';
  reason+=` Отдельный max ${fmtCall(max)} составляет ≈${(maxShare*100).toFixed(maxShare>=0.1?1:2)}% всего измеренного времени этого target за профиль; основной pattern и outlier показаны раздельно.`;
 }
 return{key,label,level,confidence,reason,load,callsPerTick:c,callsSeen:seen,avg,typicalAvg,p50,p95,p99,max,totalUs,largeOutlier,maxShare};
}
function devicePriorityScore(d){const p=devicePattern(d),load=p.load,max=p.max,p99=p.p99,c=p.callsPerTick;let score=load;score+=Math.min(200,max/25);score+=Math.min(100,p99/20);if(p.key==='burst')score+=80;if(p.key==='sustained')score+=60;if(p.key==='throughput'&&load>=25)score+=40;if(p.key==='rare-costly')score+=35;if(p.key==='bimodal'||p.key==='tail')score+=30;if(c<=0)score=0;return score}
function physicalGridShape(label){
 const devices=physicalForGrid(label).filter(d=>n(d.metric?.usPerTick)>0),total=devices.reduce((a,d)=>a+n(d.metric?.usPerTick),0),ticks=Math.max(1,n(report.profileTicks));
 let outlierLoad=0,topOutlier=null,outlierTargets=0;
 for(const d of devices){const p=devicePattern(d);if(!p.largeOutlier)continue;const contribution=Math.min(n(d.metric?.usPerTick),p.max/ticks);outlierLoad+=contribution;outlierTargets++;if(!topOutlier||p.max>topOutlier.p.max)topOutlier={d,p,contribution};}
 outlierLoad=Math.min(total,outlierLoad);const residual=Math.max(0,total-outlierLoad),outlierShare=total>0?outlierLoad/total:0;
 return{total,residual,outlierLoad,outlierShare,topOutlier,outlierTargets,active:devices.length};
}
function foreignDispatchSignal(g){const core=valueOf(g,'core'),ds=dispatchStats(g),share=ds.foreignLoad/Math.max(1,core);return{significant:ds.foreignLoad>=25&&share>=0.08,load:ds.foreignLoad,count:ds.foreignCount,share,reason:`Foreign Level dispatch ${fmt(ds.foreignLoad)} через ${ds.foreignCount} показанных foreign levels; это вторичный fan-out signal, а не primary cause и не доказательство причины лага.`}}
function gridPattern(g){
 const core=valueOf(g,'core'),services=valueOf(g,'services'),remainder=valueOf(g,'overhead'),scheduler=valueOf(g,'scheduler'),devices=valueOf(g,'devices'),phys=physicalConcentration(g,5),safeCore=Math.max(1,core),foreignSignal=foreignDispatchSignal(g);
 let key='mixed',label='Смешанная Grid',level=core>=1000?'warn':'info',confidence='medium',reason=`Core ${fmt(core)}; services ${fmt(services)}, scheduler ${fmt(scheduler)}, devices ${fmt(devices)}.`;
 if(scheduler>=25&&scheduler>=devices*1.5&&scheduler/safeCore>=0.12){key='scheduler';label='Scheduler-heavy';level=scheduler>=250?'warn':'info';confidence='high';reason=`Scheduler remainder ${fmt(scheduler)} (${pctText(scheduler,core)} Grid Core), ${calls(g?.tickManager?.queue?.callsPerTick)} queue calls.`}
 else if(devices>=25&&devices>=scheduler*1.25&&devices/safeCore>=0.12){key='devices';label='Device-heavy';level=devices>=250?'warn':'info';confidence='high';reason=`Grid devices ${fmt(devices)} (${pctText(devices,core)} Grid Core). Exact physical targets отдельно: ${fmt(phys.physicalTotal)}.`}
 else if(remainder/safeCore>=0.60&&remainder>=50){key='remainder';label='Lifecycle/remainder-heavy';level=remainder>=500?'warn':'info';confidence='low';reason=`Grid remainder ${fmt(remainder)} (${pctText(remainder,core)} Core). Это не чистый self-time: здесь реальная Grid работа + instrumentation.`}
 else if(services/safeCore>=0.65&&services>=50){key='services';label='Service-heavy';level=services>=500?'warn':'info';confidence='medium';reason=`Grid Services ${fmt(services)} (${pctText(services,core)} Core); нужен разбор конкретного service/TickManager ниже.`}
 if(foreignSignal.significant)reason+=` Secondary signal: ${foreignSignal.reason}`;
 return{key,label,level,confidence,reason,core,services,remainder,scheduler,devices,foreign:foreignSignal.load,foreignSignal:foreignSignal.significant,physical:phys.physicalTotal};
}
function gridPriorityScore(g){const p=gridPattern(g);let score=p.core;if(p.key==='scheduler')score+=p.scheduler*0.7;if(p.key==='devices')score+=p.devices*0.7;if(p.key==='remainder')score+=Math.min(200,p.remainder*0.2);if(p.foreignSignal)score+=Math.min(100,p.foreign*0.25);return score}
function patternPill(p){return `<span class="pattern-pill ${esc(p?.level||'info')}" title="${esc((p?.reason||'')+' Confidence: '+(p?.confidence||'medium'))}">${esc(p?.label||'mixed')}</span>`}
function diagnosticPriorityItems(){
 const out=[],seen=new Set(),push=x=>{if(!x||seen.has(x.key))return;seen.add(x.key);out.push(x)};
 const sustained=[...allDevices].filter(d=>{const p=devicePattern(d);return (p.key==='sustained'||p.key==='throughput')&&p.load>=10}).sort((a,b)=>devicePriorityScore(b)-devicePriorityScore(a))[0];
 if(sustained){const p=devicePattern(sustained);push({key:'device:'+deviceKey(sustained),rank:'SUSTAINED',level:p.level,title:humanType(sustained.type),value:fmt(p.load),reason:p.reason,dimension:sustained.dimension,pos:sustained.position,grid:gridByLabel(sustained.gridLabel)?sustained.gridLabel:null})}
 const burst=[...allDevices].filter(d=>devicePattern(d).key==='burst').sort((a,b)=>devicePattern(b).max-devicePattern(a).max)[0];
 if(burst){const p=devicePattern(burst);push({key:'device:'+deviceKey(burst),rank:'SPIKE',level:p.level,title:humanType(burst.type),value:`max ${fmtCall(p.max)}`,reason:p.reason,dimension:burst.dimension,pos:burst.position,grid:gridByLabel(burst.gridLabel)?burst.gridLabel:null})}
 const physicalGrid=physicalByGridStats().filter(x=>x.gridLabel!=='(unlinked)'&&x.total>=10)[0];
 if(physicalGrid){const share=globalPhysicalTotal()>0?physicalGrid.total/globalPhysicalTotal()*100:0,detail=physicalGrid.detailAvailable?'runtime Top-N detail доступен':'runtime Top-N detail не экспортирован',compact=n(reportLimits.omittedPhysicalDevices)>0?' Сумма — conservative lower bound: compact report мог omitted мелкие targets этой же Grid.':'';push({key:'physical-grid:'+physicalGrid.gridLabel,rank:'PHYSICAL',level:physicalGrid.total>=1000?'hot':physicalGrid.total>=250?'warn':'info',title:`${physicalGrid.gridLabel} physical aggregate`,value:`≥ ${fmt(physicalGrid.total)}`,reason:`${physicalGrid.active}/${physicalGrid.count} активных/экспортированных targets дают ≥${share.toFixed(1)}% exact global physical; ${detail}.${compact}`,dimension:physicalGrid.top?.dimension,pos:physicalGrid.top?.position,grid:physicalGrid.detailAvailable?physicalGrid.gridLabel:null})}
 const gridTop=[...allGrids].sort((a,b)=>gridPriorityScore(b)-gridPriorityScore(a))[0];
 if(gridTop){const p=gridPattern(gridTop);push({key:'grid:'+gridTop.label,rank:'GRID',level:p.level,title:gridTop.label,value:fmt(p.core),reason:`${p.label}: ${p.reason}`,dimension:gridTop.dimension,pos:gridTop.anchor,grid:gridTop.label})}
 const scheduler=[...allGrids].filter(g=>gridPattern(g).key==='scheduler').sort((a,b)=>valueOf(b,'scheduler')-valueOf(a,'scheduler'))[0];
 if(scheduler){const p=gridPattern(scheduler);push({key:'grid:'+scheduler.label,rank:'SCHED',level:p.level,title:scheduler.label,value:fmt(p.scheduler),reason:p.reason,dimension:scheduler.dimension,pos:scheduler.anchor,grid:scheduler.label})}
 const foreign=[...allGrids].filter(g=>foreignDispatchSignal(g).significant).sort((a,b)=>foreignDispatch(b)-foreignDispatch(a))[0];
 if(foreign){const f=foreignDispatchSignal(foreign),p=gridPattern(foreign);push({key:'dispatch:'+foreign.label,rank:'DISPATCH',level:f.load>=100?'warn':'info',title:foreign.label,value:fmt(f.load),reason:`Secondary fan-out signal; primary classification: ${p.label}. ${f.reason}`,dimension:foreign.dimension,pos:foreign.anchor,grid:foreign.label})}
 return out.slice(0,6);
}
function diagnosticPrioritiesHtml(){const items=diagnosticPriorityItems();if(!items.length)return '<div class="plain-good">Явных приоритетов по текущим эвристикам нет.</div>';return items.map((x,i)=>`<div class="priority-item"><div class="priority-rank">#${i+1} · ${esc(x.rank)}</div><div><b>${esc(x.title)}</b><div class="muted">${esc(dimShort(x.dimension))}${x.pos?' · '+esc(posText(x.pos)):''}</div></div><div>${patternPill({label:x.value,level:x.level,reason:x.reason,confidence:'report-derived'})}</div><div class="priority-reason">${esc(x.reason)}</div><div class="simple-actions">${coordActions(x.dimension,x.pos)}${x.grid?`<button data-open-grid="${esc(x.grid)}">Grid</button>`:''}</div></div>`).join('')}
function renderDiagnosticPriorities(){const html=diagnosticPrioritiesHtml();const a=document.getElementById('simplePriorities'),b=document.getElementById('diagnosticPriorities');if(a)a.innerHTML=html;if(b)b.innerHTML=html}
function currentDiagnosisItems(){const items=[];const sm=report.sampling||{};if(sm.largeServerMode){const budget=n(sm.skippedByBudget);items.push({level:budget>0?'warn':'info',title:'Large Server sampling',value:`~1/${n(sm.effectiveSampleFactor).toFixed(1)} Level callbacks · max ${n(sm.maxSampleFactor)}x`,text:`Observed ${n(sm.levelLifecycleCallbacksSeenPerProfileTick).toFixed(0)} Level lifecycle callbacks/profile tick; detailed sampled ${n(sm.levelLifecycleCallbacksSampledPerProfileTick).toFixed(0)}/tick. Physical device timing remains exact.${budget>0?` Safety budget skipped ${budget.toLocaleString()} callbacks; Grid/service estimates are conservative.`:''}`})}const dc=report.driveCoverage||{};if(Object.keys(dc).length){const r=dc.resolver||{},o=dc.operations||{},miss=n(o.unresolvedBegins),uw=n(r.unresolvedWatchers),active=n(dc.activeDriveTargets),total=n(dc.physicalDriveTargets);items.push({level:(miss>0||uw>0)?'warn':'info',title:'ME Drive coverage',value:`${active}/${total} active · ${miss.toLocaleString()} unresolved ops`,text:`Watcher cache: ${n(r.resolvedWatchers)} resolved / ${uw} unresolved; recorded-owner ${n(r.resolvedByRecordedOwner)} (stale-snapshot ${n(r.resolvedByRecordedSnapshot)}, entries ${n(r.recordedOwnerEntries)} (pre-session ${n(r.recordedOwnerEntriesAtSessionStart)}), ctor-captures during profile ${n(r.constructorOwnerCaptures)}, slot-updates ${n(r.recordedOwnerUpdates)}, failures ${n(r.recordedOwnerAccessFailures)}), storage-mount ${n(r.resolvedByStorageMountOwner)} (entries ${n(r.storageMountOwnerEntries)}, pre-session ${n(r.storageMountOwnerEntriesAtSessionStart)}, provider ids ${n(r.mountProviderIdentityEntries)}, provider passes ${n(r.storageProviderMountPasses)}, watchers ${n(r.storageMountWatchersSeen)}, failures ${n(r.storageMountAccessFailures)}), pre-map ${n(r.resolvedByHostMapping)}, stable-cell ${n(r.resolvedByCellDelegateIdentity)+n(r.resolvedByCellStackIdentity)+n(r.resolvedByCellUuid)} (delegate ${n(r.resolvedByCellDelegateIdentity)}, stack ${n(r.resolvedByCellStackIdentity)}, uuid ${n(r.resolvedByCellUuid)}; index ${n(r.cellDelegateOwnerEntries)}/${n(r.cellStackOwnerEntries)}/${n(r.cellUuidOwnerEntries)}), legacy save-provider ${n(r.resolvedByCellSaveProvider)}, direct ${n(r.resolvedByDirectOwner)}, callback-capture ${n(r.resolvedByCallbackCapture)}. Stable-cell attempts ${n(r.stableCellResolutionAttempts)}; typed fallback attempts ${n(r.typedCellResolutionAttempts)}; Basic delegates ${n(r.cellDelegateTypeBasic)}, other delegates ${n(r.cellDelegateTypeOther)}, null ${n(r.cellDelegateNull)}, delegate-access failures ${n(r.cellDelegateAccessFailures)}, save-provider failures ${n(r.cellSaveProviderAccessFailures)}, capture misses ${n(r.cellSaveProviderCaptureMisses)}. Begin calls: extract ${n(o.beginExtract).toLocaleString()}, insert ${n(o.beginInsert).toLocaleString()}, preferred ${n(o.beginPreferred).toLocaleString()}, availableStacks ${n(o.beginAvailableStacks).toLocaleString()}. Safe-skip reasons: no-accessor ${n(r.unresolvedReasons?.noOwnerAccessors)}, no-captured-drive ${n(r.unresolvedReasons?.noCapturedDrive)}, safety-reject ${n(r.unresolvedReasons?.safetyReject)}, access-failure ${n(r.unresolvedReasons?.accessFailure)}, direct-not-drive ${n(r.unresolvedReasons?.directOwnerNotDrive)}.`})}const rare=allDevices.filter(d=>n(d.metric?.callsPerTick)>0&&n(d.metric?.callsPerTick)<0.1&&avgCallUs(d.metric)>=500&&n(d.metric?.usPerTick)>=0.25).sort((a,b)=>n(b.metric?.usPerTick)-n(a.metric?.usPerTick));if(rare.length){const d=rare[0],extra=rare.length>1?` Ещё таких targets: ${rare.length-1}.`:'';items.push({level:avgCallUs(d.metric)>=2000?'hot':'warn',title:'Редкий дорогой вызов',value:`${fmtCall(avgCallUs(d.metric))} · ${fmt(d.metric?.usPerTick)}`,text:`${humanType(d.type)} · ${dimShort(d.dimension)} @ ${posText(d.position)} · ${calls(d.metric?.callsPerTick)}.${extra}`})}const spiky=allDevices.filter(d=>spikeVal(d,'maxUs')>0&&spikeVal(d,'p95Us')>0).map(d=>({d,ratio:spikeVal(d,'maxUs')/Math.max(1,spikeVal(d,'p50Us'))})).sort((a,b)=>spikeVal(b.d,'maxUs')-spikeVal(a.d,'maxUs'));if(spiky.length){const {d,ratio}=spiky[0],sp=spikeOf(d),shape=spikeShape(sp,avgCallUs(d.metric));items.push({level:sp.maxUs>=5000?'warn':'info',title:'Spike distribution',value:`P99 ${fmtCall(sp.p99Us)} · max ${fmtCall(sp.maxUs)}`,text:`${humanType(d.type)} @ ${posText(d.position)} · Avg ${fmtCall(avgCallUs(d.metric))} · max/P50 ×${ratio.toFixed(1)} · worst tick t+${n(sp.maxTick)}. ${spikeSampleNote(d)}.${shape?' '+shape:''}`})}const concs=allGrids.map(g=>({g,c:physicalConcentration(g)})).filter(x=>x.c.activeCount>=5&&x.c.physicalTotal>=1&&x.c.physicalShare>=70).sort((a,b)=>b.c.topTotal-a.c.topTotal);if(concs.length){const {g,c}=concs[0],deviceShare=c.devicesShare>0&&c.devicesShare<=125?` Это ≈${c.devicesShare.toFixed(1)}% Grid devices bucket.`:'';items.push({level:c.physicalShare>=90?'hot':'warn',title:'Нагрузка сконцентрирована',value:`Top ${c.topCount} = ${c.physicalShare.toFixed(1)}% physical`,text:`${g.label} · ${dimShort(g.dimension)}. ${fmt(c.topTotal)} из ${fmt(c.physicalTotal)} physical targets.${deviceShare}`})}const dispatch=allGrids.map(g=>({g,s:dispatchStats(g)})).filter(x=>x.s.foreignCount>=5&&x.s.foreignLoad>=1).sort((a,b)=>b.s.foreignLoad-a.s.foreignLoad);if(dispatch.length){const {g,s}=dispatch[0];items.push({level:s.foreignLoad>=25?'warn':'info',title:'Foreign Level dispatch',value:`${s.foreignCount} foreign levels · ${fmt(s.foreignLoad)}`,text:`${g.label}: anchor ${s.anchorCount} level / ${fmt(s.anchorLoad)}. Это диагностический fan-out, а не автоматическое доказательство бага AE2.`})}const risks=allGrids.map(g=>({g,r:observerRisk(g)})).filter(x=>x.r.level!=='low').sort((a,b)=>b.r.remainder-a.r.remainder);if(risks.length){const {g,r}=risks[0];items.push({level:'info',title:'Observer-risk для Grid remainder',value:`${observerRiskText(r)} · ${fmt(r.remainder)}`,text:`${g.label}: ${calls(r.callsPerTick)} Grid lifecycle calls. Remainder = Grid Core − Services и может содержать реальную AE2-работу плюс instrumentation; это риск-оценка, не измерение self-time.`})}if(!items.length)items.push({level:'info',title:'Явных current-only hotspots нет',value:'Нормальный профиль',text:'Пороговые эвристики не нашли редких дорогих вызовов, сильной концентрации physical load или заметного foreign dispatch.'});return items}
function diagnosisHtml(items){return items.map(x=>`<div class="diag-card ${x.level||'info'}"><h3>${esc(x.title)}</h3><div class="diag-value">${esc(x.value)}</div><div class="diag-text">${esc(x.text)}</div></div>`).join('')}
function renderCurrentDiagnosis(){const html=diagnosisHtml(currentDiagnosisItems());const s=document.getElementById('simpleDiagnosis'),e=document.getElementById('currentDiagnosis');if(s)s.innerHTML=html;if(e)e.innerHTML=html}


function adminPriorityLevel(score,confidence='medium'){
 if(confidence==='low'&&score<900)return {key:'watch',label:'Нужно проверить',level:'info',rank:2};
 if(score>=500)return {key:'high',label:'Высокий приоритет',level:'hot',rank:4};
 if(score>=200)return {key:'medium',label:'Средний приоритет',level:'warn',rank:3};
 if(score>=75)return {key:'watch',label:'Нужно проверить',level:'info',rank:2};
 return {key:'low',label:'Низкий приоритет',level:'info',rank:1};
}
function adminConfidenceText(v){return v==='high'?'высокая':v==='low'?'низкая':'средняя'}
function adminGridFinding(g){
 const core=valueOf(g,'core'),services=valueOf(g,'services'),scheduler=valueOf(g,'scheduler'),devices=valueOf(g,'devices'),remainder=valueOf(g,'overhead'),tm=g?.tickManager||{},tmService=n(tm.service?.usPerTick),queue=n(tm.queue?.usPerTick),queueCalls=n(tm.queue?.callsPerTick),deviceCalls=n(g?.metrics?.devices?.callsPerTick),foreign=foreignDispatchSignal(g),risk=observerRisk(g),phys=physicalByGridStats().find(x=>x.gridLabel===g.label)||null,shape=physicalGridShape(g.label);
 const combined=scheduler+devices,ratio=devices>0?scheduler/devices:(scheduler>0?99:1),safeCore=Math.max(1,core),remainderShare=remainder/safeCore,remainderMajority=remainder>=150&&remainderShare>=0.65,remainderDominant=remainder>=150&&remainderShare>=0.55&&remainder>=combined*1.5,hierarchyInconsistent=devices>=50&&((tmService>=25&&devices>tmService*1.10)||(queue>=25&&devices>queue*1.10)),remainderGuard=remainderMajority||remainderDominant,consistencyGuard=remainderGuard||hierarchyInconsistent,spikeDominated=shape.total>=50&&shape.outlierLoad>=100&&shape.outlierShare>=0.50&&devices>=50&&shape.outlierLoad>=devices*0.40;
 let cause='Смешанная работа AE2',character='Смешанная нагрузка',confidence='medium',note='Несколько measured buckets заметны одновременно; технические детали доступны в Expert mode.';
 if(remainderGuard){cause='Grid lifecycle / remainder';character='Причина отделена не полностью';confidence='low';note=remainderMajority?`Remainder занимает ${(remainderShare*100).toFixed(1)}% Grid Core, поэтому конкретный TickManager/device bucket не объявляется причиной даже если он выглядит большим.`:'Remainder заметно больше scheduler+devices вместе. Он содержит реальную Grid работу и instrumentation, поэтому конкретный TickManager bucket здесь не объявляется готовой причиной.';if(hierarchyInconsistent)note+=` Дополнительно inclusive hierarchy несогласована: devices ${fmt(devices)} больше measured TickManager service ${fmt(tmService)}${queue>0?` / queue ${fmt(queue)}`:''}; это снижает уверенность атрибуции.`}
 else if(hierarchyInconsistent){cause='Inclusive timing / несогласованные buckets';character='Причина отделена не полностью';confidence='low';note=`Device bucket ${fmt(devices)} больше measured TickManager service ${fmt(tmService)}${queue>0?` / queue ${fmt(queue)}`:''}. При такой inclusive/sampled иерархии нельзя надёжно объявлять устройства primary cause; используйте Expert detail как доказательство формы нагрузки, а не готовый вердикт.`}
 else if(spikeDominated){cause='Редкий physical spike + фон устройств';character='Spike сильно влияет на среднее';confidence='medium';note=`Крупные physical outlier-события дают ≈${(shape.outlierShare*100).toFixed(1)}% экспортированной physical-суммы этой Grid. После их удаления остаётся ${n(reportLimits.omittedPhysicalDevices)>0?'≥ ':''}${fmt(shape.residual)} exported physical background; поэтому полную device-среднюю нельзя называть постоянной нагрузкой.`}
 else if(scheduler>=75&&devices>=75&&combined>=150){const balance=ratio>=0.67&&ratio<=1.5?'scheduler + устройства':ratio>1.5?'scheduler с вкладом устройств':'устройства с вкладом scheduler';cause=`TickManager: ${balance}`;character=(queueCalls>=0.5||deviceCalls>=0.5)?'Повторяющаяся нагрузка':'Нагрузка внутри TickManager';confidence='high';note='Scheduler и device work одновременно дают значимый измеренный вклад; это полезнее общего ярлыка Service-heavy/Mixed.'}
 else if(scheduler>=50&&scheduler>=devices*1.5){cause='TickManager / scheduler';character=queueCalls>=0.5?'Повторяющаяся scheduler-нагрузка':'Scheduler-нагрузка';confidence='high';note='Основной измеренный вклад внутри TickManager приходится на scheduler remainder.'}
 else if(devices>=50&&devices>=scheduler*1.25){cause='Устройства внутри TickManager';character=deviceCalls>=0.5?'Повторяющаяся device-нагрузка':'Device-нагрузка';confidence='high';note='Основной измеренный вклад внутри TickManager приходится на device execution.'}
 else if(services>=100&&services/safeCore>=0.55){cause='Сервисы AE2 / TickManager';character='Повторяющаяся service-нагрузка';confidence='medium';note='Services занимают большую долю Grid Core; в Expert mode можно увидеть конкретный service и queue.'}
 else if(remainder>=150&&remainder/safeCore>=0.55){cause='Grid lifecycle / remainder';character='Причина отделена не полностью';confidence='low';note='Большая часть времени остаётся вне известных service buckets. Remainder может содержать реальную AE2-работу и instrumentation, поэтому это сигнал для проверки, а не готовый диагноз.'}
 const physical=phys?n(phys.total):0,adjustment=spikeDominated&&!consistencyGuard?Math.min(shape.outlierLoad,devices,queue,core):0,sustainedCore=Math.max(0,core-adjustment),sustainedDevices=Math.max(0,devices-adjustment),sustainedQueue=Math.max(0,queue-adjustment),sustainedCombined=scheduler+sustainedDevices,score=Math.max(sustainedCore*0.75,sustainedCombined,(shape.total>0?shape.residual:physical)*1.2,sustainedQueue*0.9);let priority=adminPriorityLevel(score,confidence);if(consistencyGuard&&core>=900&&priority.key==='watch'){priority={key:'medium',label:'Средний приоритет',level:'warn',rank:3};note+=' Измеренный Grid Core остаётся высоким, поэтому сеть не скрывается из Top-5: приоритет отражает масштаб, а низкая уверенность — только качество causal attribution.'}
 let evidence=[`Grid Core ${fmt(core)}`];
 if(remainderGuard){evidence.push(`remainder ${fmt(remainder)}`);if(tmService>=25)evidence.push(`TickManager service ${fmt(tmService)}`);if(devices>=25)evidence.push(`devices ${fmt(devices)}`)}
 else if(hierarchyInconsistent){evidence.push(`devices ${fmt(devices)}`);if(tmService>=25)evidence.push(`TickManager service ${fmt(tmService)}`);if(queue>=25)evidence.push(`queue ${fmt(queue)}`)}
 else if(spikeDominated){evidence.push(`devices ${fmt(devices)}`,`outlier ≈ ${fmt(shape.outlierLoad)}`,`${n(reportLimits.omittedPhysicalDevices)>0?'≥ ':''}physical без крупных outliers ${fmt(shape.residual)}`)}
 else {if(scheduler>=25)evidence.push(`scheduler ${fmt(scheduler)}`);if(devices>=25)evidence.push(`devices ${fmt(devices)}`);if(queue>=25)evidence.push(`queue ${fmt(queue)}`);if(physical>=10)evidence.push(`${n(reportLimits.omittedPhysicalDevices)>0?'≥ ':''}physical ${fmt(physical)}`)}
 if(!spikeDominated&&shape.topOutlier&&shape.outlierLoad>=25){note+=` Отдельно найден крупный physical outlier ${fmtCall(shape.topOutlier.p.max)}; после исключения известных крупных outliers exported physical остаётся ${n(reportLimits.omittedPhysicalDevices)>0?'≥ ':''}${fmt(shape.residual)}, поэтому основной sustained finding не строится только на этом spike.`}
 if(foreign.significant)note+=` Есть secondary foreign fan-out ${fmt(foreign.load)}, но он не выбран primary cause.`;
 return{key:`grid:${g.label}`,kind:'grid',grid:g.label,dimension:g.dimension,pos:g.anchor,priority,cause,character,confidence,note,evidence:evidence.slice(0,4),score,core,physical,risk:risk.level,detailAvailable:true,spikeDominated,remainderDominant,remainderMajority,hierarchyInconsistent,consistencyGuard};
}
function adminPhysicalOnlyFinding(x){const d=x.top,shape=physicalGridShape(x.gridLabel),compact=n(reportLimits.omittedPhysicalDevices)>0,share=globalPhysicalTotal()>0?x.total/globalPhysicalTotal()*100:0,score=shape.residual*1.2,priority=adminPriorityLevel(score,'medium');let cause='Физические устройства этой Grid',character='Постоянная physical нагрузка',note=`Grid не вошла в runtime Top-N detail, поэтому причина внутри Grid не классифицируется. ${compact?'Показанная сумма — нижняя граница из-за compact omissions.':'Все physical targets представлены.'}`;if(shape.topOutlier){if(shape.outlierShare>=0.50){cause='Physical load со spike-доминированием';character='Редкий spike сильно влияет на среднее';note+=` Известные крупные outliers дают ≈${(shape.outlierShare*100).toFixed(1)}% exported physical; без них остаётся ${compact?'≥ ':''}${fmt(shape.residual)}.`}else{character='Постоянная physical нагрузка + крупный outlier';note+=` Отдельный max ${fmtCall(shape.topOutlier.p.max)} виден отдельно, но после исключения известных крупных outliers остаётся ${compact?'≥ ':''}${fmt(shape.residual)} exported physical, поэтому sustained finding сохраняется.`}}const evidence=[`${compact?'≥ ':''}physical ${fmt(x.total)}`,`${compact?'≥ ':''}${share.toFixed(1)}% exact physical`,shape.topOutlier?`${compact?'≥ ':''}без крупных outliers ${fmt(shape.residual)}`:`${x.active}/${x.count} активных/экспортированных targets`,shape.topOutlier?`top outlier ${fmtCall(shape.topOutlier.p.max)}`:d?`top: ${humanType(d.type)} ${fmt(x.max)}`:''].filter(Boolean);return{key:`physical:${x.gridLabel}`,kind:'physical',grid:x.gridLabel,dimension:d?.dimension,pos:d?.position,priority,cause,character,confidence:'medium',note,evidence,score,core:0,physical:x.total,risk:'unknown',detailAvailable:false};}
function adminFindings(){const out=allGrids.map(adminGridFinding).filter(x=>x.priority.key!=='low');const existing=new Set(out.map(x=>x.grid));for(const x of physicalByGridStats()){if(x.gridLabel==='(unlinked)'||x.detailAvailable||existing.has(x.gridLabel)||x.total<50)continue;const f=adminPhysicalOnlyFinding(x);if(f.priority.key!=='low')out.push(f)}out.sort((a,b)=>b.priority.rank-a.priority.rank||b.score-a.score||b.physical-a.physical);return out.slice(0,5)}
function adminRareEvents(){return [...allDevices].map(d=>({d,p:devicePattern(d),max:spikeVal(d,'maxUs'),seen:n(spikeOf(d)?.callsSeen),c:n(d.metric?.callsPerTick)})).filter(x=>x.max>=300&&(x.p.key==='burst'||x.p.largeOutlier||x.seen<=3||x.c<0.1)).sort((a,b)=>b.max-a.max).slice(0,3)}
function adminFindingHtml(x,i){const location=x.pos?`${dimShort(x.dimension)} · ${posText(x.pos)}`:dimShort(x.dimension),where=x.kind==='physical'?'координаты самого заметного exported target':'anchor Grid';return `<div class="admin-finding ${x.priority.level}"><div class="admin-finding-head"><div><span class="admin-priority ${x.priority.level}">#${i+1} · ${esc(x.priority.label)}</span><h3>${esc(x.grid)}</h3><div class="admin-meta"><span>${esc(location)}</span><span>${esc(where)}</span><span>уверенность: <b>${esc(adminConfidenceText(x.confidence))}</b></span></div></div><div>${patternPill({label:x.character,level:x.priority.level,reason:x.note,confidence:x.confidence})}</div></div><div class="admin-cause">Причина: ${esc(x.cause)}</div><div class="admin-character">${esc(x.note)}</div><div class="admin-evidence">${x.evidence.map(e=>`<span>${esc(e)}</span>`).join('')}</div><div class="simple-actions">${coordActions(x.dimension,x.pos)}${x.detailAvailable?`<button data-open-grid="${esc(x.grid)}">Технические подробности</button>`:''}</div></div>`}
function adminSpikeHtml(x){const d=x.d,p=x.p,rare=x.seen<=3||x.c<0.1,activeOutlier=p.largeOutlier&&!rare;const msg=activeOutlier?'На фоне частых вызовов обнаружен отдельный крупный outlier. Постоянный поток и этот spike показаны раздельно; не считать всю среднюю нагрузку единичным событием.':rare?'Редкое событие: не считать постоянным источником нагрузки без повторяемости.':'Есть дорогой хвост вызовов, но он не является главным sustained finding.';return `<div class="admin-spike"><h3>${esc(humanType(d.type))}</h3><div class="diag-value">max ${fmtCall(x.max)}</div><div class="admin-meta"><span>${esc(dimShort(d.dimension))} · ${esc(posText(d.position))}</span><span>${x.seen.toLocaleString()} calls за профиль</span></div><div class="admin-note">${msg} Средняя нагрузка ${fmt(d.metric?.usPerTick)}.</div><div class="simple-actions">${coordActions(d.dimension,d.position)}${d.gridLabel&&gridByLabel(d.gridLabel)?`<button data-open-grid="${esc(d.gridLabel)}">Технические подробности</button>`:''}</div></div>`}
function adminSummaryText(){const findings=adminFindings(),rare=adminRareEvents(),high=findings.filter(x=>x.priority.key==='high').length,medium=findings.filter(x=>x.priority.key==='medium').length,top=findings[0],o=report.driveCoverage?.operations||{};const lines=[`Observable AE2 Admin Report v20.5.3`,`Профиль: ${n(report.profileTicks)} ticks / ${n(report.profileWallClockMs)} ms, observed ${n(report.observedProfileTps).toFixed(2)} TPS`,`Приоритеты: высоких ${high}, средних ${medium}, всего показано ${findings.length}; редких событий ${rare.length}`,`Drive coverage: ${n(o.resolvedBegins)}/${n(o.begins)} resolved, unresolved ${n(o.unresolvedBegins)}`];if(top)lines.push(`Главное: ${top.grid} — ${top.cause}; ${top.character}; ${top.evidence.join(', ')}`);for(const [i,x] of findings.entries())lines.push(`${i+1}. ${x.priority.label}: ${x.grid} — ${x.cause}; ${x.character}; ${x.evidence.join(', ')}`);if(rare.length)lines.push(`Редкие события: ${rare.map(x=>`${humanType(x.d.type)} ${dimShort(x.d.dimension)} @ ${posText(x.d.position)} max ${fmtCall(x.max)}`).join('; ')}`);return lines.join('\\n')}
function renderAdminReport(){const findings=adminFindings(),rare=adminRareEvents(),high=findings.filter(x=>x.priority.key==='high').length,medium=findings.filter(x=>x.priority.key==='medium').length,watch=findings.filter(x=>x.priority.key==='watch').length,o=report.driveCoverage?.operations||{},coverage=n(o.begins)>0&&n(o.unresolvedBegins)===0,top=findings[0],scout=globalTotals.gridTotalsScope==='scout-estimate';document.getElementById('simpleHero').innerHTML=top?`<h2>Итог для администрации</h2><div class="big ${top.priority.level==='hot'?'plain-hot':top.priority.level==='warn'?'plain-warn':'plain-good'}">${esc(top.priority.label)} · ${esc(top.grid)}</div><div class="lead"><b>${esc(top.cause)}.</b> ${esc(top.character)}. Сначала разберите эту сеть; остальные приоритеты ниже. ${scout?'Global Grid цифра сверху в Expert mode — scout estimate, не полный MSPT сервера.':''}</div>`:`<h2>Итог для администрации</h2><div class="big plain-good">Явных AE2-приоритетов не найдено</div><div class="lead">Текущие report-derived пороги не нашли заметной sustained Grid/physical нагрузки. Редкие события показаны отдельно и не считаются постоянной причиной.</div>`;document.getElementById('simpleCards').innerHTML=`<div class="card"><small>Высокий приоритет</small><b class="${high?'plain-hot':'plain-good'}">${high}</b></div><div class="card"><small>Средний приоритет</small><b class="${medium?'plain-warn':'plain-good'}">${medium}</b></div><div class="card"><small>Нужно проверить</small><b>${watch}</b></div><div class="card"><small>Drive coverage</small><b class="${coverage?'plain-good':'plain-hot'}">${coverage?'OK':'WARN'}</b><small>${n(o.resolvedBegins).toLocaleString()}/${n(o.begins).toLocaleString()} resolved</small></div>`;document.getElementById('adminFindings').innerHTML=findings.length?findings.map(adminFindingHtml).join(''):'<div class="admin-good">По текущим порогам нет сетей, которые надо поднимать в административный Top-5. Экспертные данные сохранены.</div>';document.getElementById('adminSpikes').innerHTML=rare.length?rare.map(adminSpikeHtml).join(''):'<div class="admin-good">Редких физических событий ≥300 µs, подходящих под административный фильтр, в этом профиле нет.</div>';}

function renderOverview(){const s=globalStats();const physicalTotal=globalPhysicalTotal();renderCurrentDiagnosis();renderDiagnosticPriorities();renderSpikeAnalysis();document.getElementById('globalCards').innerHTML=`<div class="card"><small>Σ Grid Core</small><b>${fmt(s.core)}</b></div><div class="card"><small>Σ Grid Devices</small><b>${fmt(s.devices)}</b></div><div class="card"><small>Σ Physical targets</small><b>${fmt(physicalTotal)}</b></div><div class="card"><small>AE2 Grids</small><b>${s.grids.toLocaleString()}</b></div><div class="card"><small>Linked physical</small><b>${s.linked}/${observedPhysicalCount()}</b></div>`;
 const top=[...allGrids].sort((a,b)=>valueOf(b,'core')-valueOf(a,'core')).slice(0,20);const tg=document.getElementById('topGrids');tg.innerHTML=gridRows(top);bindGridRows(tg,top);
 const dims=dimensionStats().slice(0,15);document.getElementById('dimensionSummary').innerHTML=`<div class="table-scroll compact"><table class="data-table"><thead><tr><th>Dimension</th><th>Grids</th><th>Σ Core</th><th>Share</th><th>Max Core</th><th>Σ Devices</th></tr></thead><tbody>${dims.map(x=>`<tr class="clickable"><td class="left dim" title="${esc(x.dimension)}">${esc(dimShort(x.dimension))}</td><td>${x.count}</td><td>${fmt(x.core)}</td><td>${pctText(x.core,s.core)}</td><td>${fmt(x.max)}</td><td>${fmt(x.devices)}</td></tr>`).join('')}</tbody></table></div>`;document.querySelectorAll('#dimensionSummary tbody tr').forEach((tr,i)=>tr.onclick=()=>{document.getElementById('gridDimension').value=dims[i].dimension;switchView('grids')});
 const devices=[...allDevices].sort((a,b)=>n(b.metric?.usPerTick)-n(a.metric?.usPerTick));document.getElementById('topDevices').innerHTML=devices.length?deviceRows(devices,20):'<div class="empty">В этом отчёте нет physicalDevices.</div>';
 const pg=physicalByGridStats().slice(0,20),compactPhysical=n(reportLimits.omittedPhysicalDevices)>0;document.getElementById('physicalByGrid').innerHTML=pg.length?`<div class="mode-hint">Per-Grid sum uses exported physical targets; ${compactPhysical?'with compact omissions it is a conservative lower bound (≥ share of exact global physical).':'all physical targets are represented in the report.'}</div><div class="table-scroll compact"><table class="data-table"><thead><tr><th>Grid</th><th>Active/exported</th><th>Σ exported physical</th><th>Exact-global share</th><th>Grid detail</th><th>Max target</th></tr></thead><tbody>${pg.map(x=>`<tr class="${x.detailAvailable?'clickable':''}" ${x.detailAvailable?`data-open-grid="${esc(x.gridLabel)}"`:''}><td class="left">${x.gridLabel==='(unlinked)'?'<span class="muted">unlinked</span>':esc(x.gridLabel)}</td><td>${x.active}/${x.count}</td><td>${compactPhysical&&x.gridLabel!=='(unlinked)'?'≥ ':''}${fmt(x.total)}</td><td>${compactPhysical&&x.gridLabel!=='(unlinked)'?'≥ ':''}${pctText(x.total,physicalTotal)}</td><td>${x.gridLabel==='(unlinked)'?'—':x.detailAvailable?'<span class="plain-good">Top-N detail</span>':'<span class="plain-warn">physical-only</span>'}</td><td>${fmt(x.max)}</td></tr>`).join('')}</tbody></table></div>`:'<div class="empty">Нет physical devices</div>';
 const types=deviceTypeStats().slice(0,20);document.getElementById('hotspotTypes').innerHTML=types.length?`<div class="table-scroll compact"><table class="data-table"><thead><tr><th>Type</th><th>Positions</th><th>Σ load</th><th>Share</th><th>Avg/call</th></tr></thead><tbody>${types.map(x=>`<tr><td class="left clip" title="${esc(x.type)}">${esc(x.type)}</td><td>${x.count}</td><td>${fmt(x.total)}</td><td>${pctText(x.total,physicalTotal)}</td><td>${fmtCall(x.avgCall)}</td></tr>`).join('')}</tbody></table></div>`:'<div class="empty">Нет данных</div>';
}
function physicalByGridStats(){const map=new Map();for(const d of allDevices){const k=d.gridLabel||'(unlinked)';let x=map.get(k);if(!x){x={gridLabel:k,count:0,active:0,total:0,max:0,top:null,detailAvailable:false};map.set(k,x)}const v=n(d.metric?.usPerTick);x.count++;if(v>0)x.active++;x.total+=v;if(v>x.max){x.max=v;x.top=d}}for(const x of map.values())x.detailAvailable=x.gridLabel!=='(unlinked)'&&!!gridByLabel(x.gridLabel);return [...map.values()].sort((a,b)=>b.total-a.total)}
function deviceTypeStats(devices=allDevices){const map=new Map();for(const d of devices){const k=d.type||'unknown';let x=map.get(k);if(!x){x={type:k,count:0,total:0,max:0,nanos:0,calls:0};map.set(k,x)}const v=n(d.metric?.usPerTick);x.count++;x.total+=v;x.max=Math.max(x.max,v);x.nanos+=n(d.metric?.totalNanos);x.calls+=n(d.metric?.calls)}for(const x of map.values())x.avgCall=x.calls>0?x.nanos/1000/x.calls:0;return [...map.values()].sort((a,b)=>b.total-a.total)}
const pctDelta=(cur,base)=>base>0?(cur-base)/base*100:(cur>0?Infinity:0);
const signedPct=p=>Number.isFinite(p)?`${p>0?'+':''}${p.toFixed(Math.abs(p)>=100?0:1)}%`:(p>0?'from 0':'—');
function metricRegressionInsight(cur,base){
 const cLoad=n(cur?.usPerTick),bLoad=n(base?.usPerTick),cCalls=n(cur?.callsPerTick),bCalls=n(base?.callsPerTick),cCost=avgCallUs(cur),bCost=avgCallUs(base);
 if(cLoad<=bLoad)return {kind:'none',text:''};
 if(bLoad<=0)return {kind:'new',text:`Нагрузка появилась: ${fmt(cLoad)}; baseline для этой метрики был 0.`};
 if(bCalls<=0&&cCalls>0)return {kind:'activation',text:`Метрика начала вызываться: ${calls(cCalls)} при ${fmtCall(cCost)} за вызов.`};
 const callsRatio=bCalls>0?cCalls/bCalls:1,costRatio=bCost>0?cCost/bCost:1,callsUp=callsRatio>=1.15,costUp=costRatio>=1.15,callsDown=callsRatio<=0.85,costDown=costRatio<=0.85;
 const callPart=`частота ${calls(bCalls)} → ${calls(cCalls)} (${signedPct(pctDelta(cCalls,bCalls))})`;
 const costPart=`cost/call ${fmtCall(bCost)} → ${fmtCall(cCost)} (${signedPct(pctDelta(cCost,bCost))})`;
 if(callsUp&&costUp){const cm=Math.abs(Math.log(Math.max(callsRatio,1e-9))),km=Math.abs(Math.log(Math.max(costRatio,1e-9)));if(cm>=km*1.5)return {kind:'frequency',text:`Основной драйвер — более частые вызовы: ${callPart}; ${costPart}.`};if(km>=cm*1.5)return {kind:'cost',text:`Основной драйвер — подорожание одного вызова: ${costPart}; ${callPart}.`};return {kind:'mixed',text:`Рост смешанный: вызовы стали и чаще, и дороже; ${callPart}; ${costPart}.`}}
 if(callsUp)return {kind:'frequency',text:`Основной драйвер — более частые вызовы: ${callPart}; ${costPart}${costDown?' (один вызов при этом подешевел)':''}.`};
 if(costUp)return {kind:'cost',text:`Основной драйвер — подорожание одного вызова: ${costPart}; ${callPart}${callsDown?' (вызовов при этом стало меньше)':''}.`};
 return {kind:'mixed',text:`Нагрузка выросла без одного доминирующего фактора; ${callPart}; ${costPart}.`};
}
function reportDevicesForGrid(r,label){return (r?.physicalDevices||[]).filter(d=>d.gridLabel===label)}
function typeCountText(devices,prefix){const map=new Map();for(const d of devices)map.set(d.type||'unknown',(map.get(d.type||'unknown')||0)+1);const rows=[...map.entries()].sort((a,b)=>b[1]-a[1]);const shown=rows.slice(0,3).map(([type,count])=>`${prefix}${count} ${humanType(type)}`);const hidden=rows.slice(3).reduce((a,x)=>a+x[1],0);if(hidden)shown.push(`${prefix}${hidden} других`);return shown.join(', ')}
function gridTopologyDiff(x){if(!x?.cur||!x?.base||!baselineReport)return {added:[],removed:[]};const cur=reportDevicesForGrid(report,x.cur.label),base=reportDevicesForGrid(baselineReport,x.base.label),cm=new Map(cur.map(d=>[deviceKey(d),d])),bm=new Map(base.map(d=>[deviceKey(d),d]));return {added:[...cm.entries()].filter(([k])=>!bm.has(k)).map(([,d])=>d),removed:[...bm.entries()].filter(([k])=>!cm.has(k)).map(([,d])=>d)}}
function topologyText(t){const parts=[];if(t.added.length)parts.push(typeCountText(t.added,'+'));if(t.removed.length)parts.push(typeCountText(t.removed,'-'));return parts.join('; ')}
function gridRegressionInsight(x){
 if(!x?.base)return 'Новая Grid: в baseline этой сети не было.';
 if(!x?.cur)return 'Grid отсутствует в текущем профиле.';
 const parts=[];
 if(x.delta>0){const comps=[['устройства сети','devices'],['планировщик AE2','scheduler'],['служебная работа Grid','overhead']].map(([name,kind])=>({name,kind,cv:valueOf(x.cur,kind),bv:valueOf(x.base,kind)})).map(z=>({...z,delta:z.cv-z.bv})).filter(z=>z.delta>0).sort((a,b)=>b.delta-a.delta);if(comps[0])parts.push(`Самый заметный измеренный рост внутри Grid — ${comps[0].name}: ${deltaText(comps[0].delta)} (${fmt(comps[0].bv)} → ${fmt(comps[0].cv)}).`);const core=metricRegressionInsight(x.cur.metrics?.gridCore,x.base.metrics?.gridCore);if(core.text)parts.push(`Grid Core: ${core.text}`)}
 const topo=gridTopologyDiff(x),tt=topologyText(topo);if(tt)parts.push(`Topology: ${tt}. Изменение состава совпало с изменением профиля, но само по себе не доказывает причинность.`);
 return parts.join(' ')||'По доступным метрикам явный драйвер изменения не выделяется.';
}
function deviceRegressionInsight(x){if(!x?.base)return 'Новое физическое устройство в текущем профиле.';if(!x?.cur)return 'Устройство отсутствует в текущем профиле.';const insight=metricRegressionInsight(x.cur.metric,x.base.metric);return insight.text||''}
function renderRegressionInsights(gd,dd){
 const el=document.getElementById('compareInsights');if(!el)return;
 const grids=gd.filter(x=>x.status==='regression').sort((a,b)=>b.delta-a.delta).slice(0,3),devices=dd.filter(x=>x.status==='regression').sort((a,b)=>b.delta-a.delta).slice(0,3);
 if(!grids.length&&!devices.length){el.innerHTML='<div class="plain-good">Сильных регрессий для объяснения не найдено.</div>';return}
""");
        html.append("""
 const gh=grids.map(x=>{const g=x.cur||x.base;return `<div class="simple-issue"><h3>${esc(g?.label||'Grid')} <span class="delta-up">+${fmt(x.delta)}</span></h3><div class="simple-why">${esc(gridRegressionInsight(x))}</div></div>`}).join('');
 const dh=devices.map(x=>{const d=x.cur||x.base;return `<div class="simple-device"><div><b>${esc(humanType(d?.type))}</b><div class="where">${esc(dimShort(d?.dimension))} · ${esc(posText(d?.position))}</div></div><div><b class="delta-up">+${fmt(x.delta)}</b><div class="muted">${fmt(x.bv)} → ${fmt(x.cv)}</div></div><div>${esc(deviceRegressionInsight(x))}</div><div class="simple-actions">${coordActions(d?.dimension,d?.position)}</div></div>`}).join('');
 el.innerHTML=`${gh?`<h3>Главные Grid-регрессии</h3><div class="simple-issues">${gh}</div>`:''}${dh?`<h3 style="margin-top:14px">Главные device-регрессии</h3><div class="simple-list">${dh}</div>`:''}`;
}
function saveUiState(){try{localStorage.setItem(UI_STORAGE_KEY,JSON.stringify({mode:siteMode,view:currentView,gridState,deviceState,compareState}))}catch(e){}}
function restoreUiState(){try{const s=JSON.parse(localStorage.getItem(UI_STORAGE_KEY)||'null');if(!s)return;if(s.gridState)Object.assign(gridState,s.gridState);if(s.deviceState)Object.assign(deviceState,s.deviceState);if(s.compareState)Object.assign(compareState,s.compareState);if(['overview','grids','devices','compare'].includes(s.view))currentView=s.view;if(['simple','expert'].includes(s.mode))siteMode=s.mode}catch(e){}}
function setSelectValue(id,value,fallback='*'){const el=document.getElementById(id);if([...el.options].some(o=>o.value===String(value)))el.value=String(value);else el.value=fallback}
function syncStateToControls(){document.getElementById('gridSearch').value=gridState.q||'';setSelectValue('gridDimension',gridState.dimension);document.getElementById('gridMin').value=gridState.min;setSelectValue('gridSort',gridState.sort,'core');setSelectValue('gridTop',gridState.top,'50');document.getElementById('gridActive').checked=!!gridState.active;document.getElementById('deviceSearch').value=deviceState.q||'';setSelectValue('deviceDimension',deviceState.dimension);document.getElementById('deviceMin').value=deviceState.min;setSelectValue('deviceSort',deviceState.sort,'time');setSelectValue('deviceTop',deviceState.top,'50');setSelectValue('compareKind',compareState.kind,'all');setSelectValue('compareSort',compareState.sort,'delta');document.getElementById('comparePct').value=compareState.minPct;document.getElementById('compareAbs').value=compareState.minAbs;setSelectValue('compareTop',compareState.top,'100')}
function dimensionStats(){const map=new Map();for(const g of allGrids){const d=g.dimension||'unknown';let x=map.get(d);if(!x){x={dimension:d,count:0,core:0,max:0,devices:0,scheduler:0};map.set(d,x)}const c=valueOf(g,'core');x.count++;x.core+=c;x.max=Math.max(x.max,c);x.devices+=valueOf(g,'devices');x.scheduler+=valueOf(g,'scheduler')}return [...map.values()].sort((a,b)=>b.core-a.core)}
function globalStats(){return{core:globalValue('gridCoreUsPerTick',allGrids.reduce((a,g)=>a+valueOf(g,'core'),0)),devices:globalValue('gridDevicesUsPerTick',allGrids.reduce((a,g)=>a+valueOf(g,'devices'),0)),scheduler:globalValue('schedulerUsPerTick',allGrids.reduce((a,g)=>a+valueOf(g,'scheduler'),0)),grids:globalValue('observedGrids',allGrids.length),dims:globalValue('observedDimensions',new Set(allGrids.map(g=>g.dimension||'unknown')).size),linked:globalValue('linkedPhysicalDevices',allDevices.filter(d=>d.gridLabel).length)}}
function gridRows(grids,limit=0){const total=globalStats().core;const rows=(limit?grids.slice(0,limit):grids).map(g=>{const gp=gridPattern(g);return `<tr class="clickable" data-grid="${esc(g.label+'|'+g.dimension+'|'+anchorText(g))}"><td class="left">${badge(valueOf(g,'core'))} <b>${esc(g.label)}</b></td><td>${fmt(valueOf(g,'core'))}</td><td>${pctText(valueOf(g,'core'),total)}</td><td class="left">${patternPill(gp)}</td><td>${fmt(valueOf(g,'devices'))}</td><td>${fmt(valueOf(g,'scheduler'))}</td><td>${fmt(valueOf(g,'overhead'))}</td><td class="left dim" title="${esc(g.dimension)}">${esc(dimShort(g.dimension))}</td><td>${esc(anchorText(g))}</td><td>${coordActions(g.dimension,g.anchor)}</td></tr>`}).join('');return `<div class="table-scroll compact"><table class="data-table"><thead><tr><th>Grid</th><th>Core</th><th>Share</th><th class="left">Pattern</th><th>Devices</th><th>Scheduler</th><th>Remainder*</th><th class="left">Dimension</th><th>Anchor</th><th>Actions</th></tr></thead><tbody>${rows}</tbody></table></div>`}
function bindGridRows(root,grids){root.querySelectorAll('tbody tr').forEach((tr,i)=>tr.onclick=e=>{if(e.target.closest('[data-copy-text]'))return;selectedGrid=grids[i];switchView('grids')})}
function deviceRows(devices,limit=0){const arr=limit?devices.slice(0,limit):devices;const total=globalPhysicalTotal();const rows=arr.map(d=>{const dp=devicePattern(d);return `<tr><td class="left clip" title="${esc(d.type)}">${badge(d.metric?.usPerTick)} <b>${esc(d.type||'unknown')}</b></td><td>${fmt(d.metric?.usPerTick)}</td><td>${pctText(d.metric?.usPerTick,total)}</td><td class="left">${patternPill(dp)}</td><td>${fmtCall(avgCallUs(d.metric))}</td><td>${fmtCall(spikeVal(d,'p50Us'))}</td><td>${fmtCall(spikeVal(d,'p95Us'))}</td><td>${fmtCall(spikeVal(d,'p99Us'))}</td><td>${fmtCall(spikeVal(d,'maxUs'))}</td><td>${n(spikeOf(d)?.callsSeen).toLocaleString()}</td><td>${calls(d.metric?.callsPerTick)}</td><td>${gridLink(d.gridLabel)}</td><td class="left dim" title="${esc(d.dimension)}">${esc(dimShort(d.dimension))}</td><td>${esc(posText(d.position))}</td><td>${coordActions(d.dimension,d.position)}</td></tr>`}).join('');return `<div class="table-scroll compact"><table class="data-table"><thead><tr><th>Physical device</th><th>Load</th><th>Share</th><th class="left">Pattern</th><th>Avg</th><th>P50</th><th>P95</th><th>P99</th><th>Max</th><th>Samples</th><th>Calls</th><th>Grid</th><th class="left">Dimension</th><th>Position</th><th>Actions</th></tr></thead><tbody>${rows}</tbody></table></div>`}
function simpleDeviceHtml(d){const load=n(d.metric?.usPerTick),dp=devicePattern(d);return `<div class="simple-device"><div><b>${esc(humanType(d.type))}</b><div class="where">${esc(dimShort(d.dimension))} · ${esc(posText(d.position))} · ${d.gridLabel?esc(d.gridLabel):'unlinked'}</div></div><div><b>${fmt(load)}</b><div class="muted">${fmtCall(avgCallUs(d.metric))}</div></div><div>${patternPill(dp)}<div class="muted" style="margin-top:4px">${esc(dp.reason)}</div></div><div class="simple-actions">${coordActions(d.dimension,d.position)}${d.gridLabel&&gridByLabel(d.gridLabel)?`<button data-open-grid="${esc(d.gridLabel)}">Открыть сеть</button>`:''}</div></div>`}
function renderSimple(){renderAdminReport();renderSimpleCompare()}
function applyGridFilters(sync=true){if(sync){gridState.q=document.getElementById('gridSearch').value.trim().toLowerCase();gridState.dimension=document.getElementById('gridDimension').value;gridState.min=Math.max(0,n(document.getElementById('gridMin').value));gridState.sort=document.getElementById('gridSort').value;gridState.top=parseInt(document.getElementById('gridTop').value||'0',10)||0;gridState.active=document.getElementById('gridActive').checked}
 let arr=allGrids.filter(g=>{if(gridState.dimension!=='*'&&(g.dimension||'unknown')!==gridState.dimension)return false;if(valueOf(g,'core')<gridState.min)return false;if(gridState.active&&valueOf(g,'devices')<=0)return false;if(gridState.q){const hay=`${g.label} ${g.dimension} ${anchorText(g)}`.toLowerCase();if(!hay.includes(gridState.q))return false}return true});const matched=arr.length;arr.sort((a,b)=>gridState.sort==='priority'?gridPriorityScore(b)-gridPriorityScore(a):valueOf(b,gridState.sort)-valueOf(a,gridState.sort)||valueOf(b,'core')-valueOf(a,'core'));if(gridState.top>0)arr=arr.slice(0,gridState.top);gridFiltered=arr;if(!selectedGrid||!arr.includes(selectedGrid))selectedGrid=arr[0]||null;renderGridStatus(matched);renderGridList();renderSelection();renderGridDetail();saveUiState()}
function renderGridStatus(matched){document.getElementById('gridStatus').innerHTML=`<span>Показано <strong>${gridFiltered.length}</strong> из <strong>${allGrids.length}</strong> detailed grids</span><span>Observed globally: <strong>${globalStats().grids}</strong></span><span>Фильтру соответствуют: ${matched}</span><span>Core ≥ ${gridState.min} µs/t</span>`}
function renderGridList(){const root=document.getElementById('gridList');if(!gridFiltered.length){root.innerHTML='<div class="panel empty">По текущему фильтру сетей нет.</div>';return}root.innerHTML=gridRows(gridFiltered);root.querySelectorAll('tbody tr').forEach((tr,i)=>{if(gridFiltered[i]===selectedGrid)tr.classList.add('selected');tr.onclick=e=>{if(e.target.closest('[data-copy-text]'))return;selectedGrid=gridFiltered[i];renderGridList();renderSelection();renderGridDetail()}})}
function renderSelection(){const g=selectedGrid,root=document.getElementById('selectionSummary');if(!g){root.innerHTML='<div class="empty">Сеть не выбрана</div>';return}const ds=dispatchStats(g),risk=observerRisk(g);root.innerHTML=`<div class="muted">${esc(g.label)} · ${esc(dimShort(g.dimension))}</div><div class="big">${fmt(valueOf(g,'core'))}</div><div class="kv"><span>Devices</span><b>${fmt(valueOf(g,'devices'))}</b><span>Scheduler</span><b>${fmt(valueOf(g,'scheduler'))}</b><span>Grid remainder*</span><b>${fmt(valueOf(g,'overhead'))}</b><span>Foreign dispatch</span><b>${ds.foreignCount} · ${fmt(ds.foreignLoad)}</b><span>Observer-risk*</span><b class="${observerRiskClass(risk)}">${esc(observerRiskText(risk))}</b><span>Services</span><b>${fmt(valueOf(g,'services'))}</b><span>Anchor</span><b>${esc(anchorText(g))}</b></div><div class="mode-hint">* Remainder и observer-risk — диагностические производные; self-time profiler отдельно не измеряется.</div><div style="margin-top:10px">${coordActions(g.dimension,g.anchor)}</div>`}
function renderGridDetail(){const g=selectedGrid,root=document.getElementById('gridDetail');if(!g){root.innerHTML='';return}const m=g.metrics||{},tm=g.tickManager,core=Math.max(1,n(m.gridCore?.usPerTick)),ds=dispatchStats(g),risk=observerRisk(g),conc=physicalConcentration(g),gp=gridPattern(g);let html=`<div class="meta coord-line"><span>${esc(g.dimension)}</span><span>anchor: ${esc(anchorText(g))}</span>${coordActions(g.dimension,g.anchor)}</div><div class="cards"><div class="card"><small>Grid Core</small><b>${fmt(m.gridCore?.usPerTick)}</b></div><div class="card"><small>Devices</small><b>${fmt(m.devices?.usPerTick)}</b></div><div class="card"><small>Scheduler</small><b>${fmt(tm?.schedulerRemainder?.usPerTick)}</b></div><div class="card"><small>Grid remainder*</small><b>${fmt(m.gridOverhead?.usPerTick)}</b></div><div class="card"><small>Foreign dispatch</small><b>${fmt(ds.foreignLoad)}</b><small>${ds.foreignCount} levels</small></div><div class="card"><small>Observer-risk*</small><b class="${observerRiskClass(risk)}">${esc(observerRiskText(risk))}</b><small>${calls(risk.callsPerTick)} lifecycle</small></div><div class="card"><small>Services</small><b>${fmt(m.gridServices?.usPerTick)}</b></div><div class="card"><small>Diagnostic pattern</small><b>${esc(gp.label)}</b><small>confidence: ${esc(gp.confidence)}</small></div></div><div class="note observer-note"><b>* Interpretation:</b> Grid remainder = Grid Core − Grid Services. Он не является чистым "AE2 overhead": туда может попадать реальная работа Grid и стоимость instrumentation. Observer-risk — консервативная эвристика, а не измеренный self-time profiler.${conc.activeCount>=5&&conc.physicalShare>=70?` Top ${conc.topCount} physical targets дают ${conc.physicalShare.toFixed(1)}% physical load этой Grid (${fmt(conc.topTotal)} из ${fmt(conc.physicalTotal)}).`:''}</div><section><div class="panel-head"><h2>Диагностическая классификация Grid</h2><span class="muted">report-derived classification</span></div><div>${patternPill(gp)}</div><div class="simple-why">${esc(gp.reason)}</div><div class="meta"><span>Core: <b>${fmt(gp.core)}</b></span><span>Services: <b>${fmt(gp.services)}</b></span><span>Scheduler: <b>${fmt(gp.scheduler)}</b></span><span>Grid devices: <b>${fmt(gp.devices)}</b></span><span>Foreign dispatch: <b>${fmt(gp.foreign)}</b></span><span>Exact physical: <b>${fmt(gp.physical)}</b></span></div></section>`;
 html+=section('Overview',metric('Grid Core (inclusive)',m.gridCore,core,0,true)+metric('Grid Services (sum)',m.gridServices,core,1)+metric('Grid remainder* (derived)',m.gridOverhead,core,1)+metric('Devices',m.devices,core,1));const services=(g.services||[]).slice().sort((a,b)=>n(b.total?.usPerTick)-n(a.total?.usPerTick));html+=section('Services',services.map(s=>metric(`${s.name} · ${s.className}`,s.total,Math.max(1,n(m.gridServices?.usPerTick)))).join(''));const gridDevices=allDevices.filter(d=>d.gridLabel===g.label).sort((a,b)=>n(b.metric?.usPerTick)-n(a.metric?.usPerTick));html+=section(`Top physical devices in ${g.label}`,gridDevices.length?deviceRows(gridDevices,25):'<div class="empty">Для этой Grid физические timing targets пока не связаны. Пассивные или неактивные устройства могут остаться unlinked.</div>');
 let adv='';const cp=g.corePhases||{};adv+=section('Grid Core phases',metric('Server start',cp.server_start,core)+metric('Level start',cp.level_start,core)+metric('Level end',cp.level_end,core)+metric('Server end',cp.server_end,core));if(tm){const max=Math.max(1,n(tm.service?.usPerTick));adv+=section('Tick Manager',metric('Service (inclusive)',tm.service,max,0,true)+metric('Level queue',tm.levelQueue,max,1)+metric('Queue (inclusive)',tm.queue,max,1,true)+metric('Devices',tm.devices,max,2)+metric('Scheduler remainder',tm.schedulerRemainder,max,2,true)+metric('Queue residual + guard',tm.queueResidual,max,2)+metric('Level dispatch overhead',tm.levelDispatchOverhead,max,1)+metric('Outer overhead',tm.outerOverhead,max,1));const dl=(tm.dispatchLevels||[]).slice().sort((a,b)=>n(b.total?.callsPerTick)-n(a.total?.callsPerTick)||n(b.total?.usPerTick)-n(a.total?.usPerTick));if(dl.length){const same=dl.filter(x=>x.anchorDimension),foreign=dl.filter(x=>!x.anchorDimension);const sum=(arr,key)=>arr.reduce((a,x)=>a+n(x.total?.[key]),0);adv+=`<section><h2>Actual Level dispatch</h2><div class="meta"><span>Levels: <b>${n(tm.dispatchLevelsTotal)||dl.length}</b>${n(tm.dispatchLevelsOmitted)>0?` · shown ${dl.length}`:''}</span><span class="anchor">Anchor shown: ${same.length}</span><span class="foreign">Foreign shown: ${foreign.length}</span></div><div class="dispatch-scroll"><table class="data-table"><thead><tr><th>Actual Level</th><th>Relation</th><th>Total</th><th>Calls</th><th>LevelEnd</th></tr></thead><tbody>${dl.map(x=>`<tr><td class="left dim" title="${esc(x.dimension)}">${esc(dimShort(x.dimension))}</td><td class="${x.anchorDimension?'anchor':'foreign'}">${x.anchorDimension?'anchor':'foreign'}</td><td>${fmt(x.total?.usPerTick)}</td><td>${calls(x.total?.callsPerTick)}</td><td>${fmt(x.levelEnd?.usPerTick)}</td></tr>`).join('')}</tbody></table></div></section>`}const q=tm.queuePhases||{},qmax=Math.max(1,n(tm.queue?.usPerTick));adv+=section('Tick Queue phases',metric('Head due-check',q.headDueCheck,qmax)+metric('Poll + dequeue prep',q.dequeuePrep,qmax)+metric('Tick-rate update',q.rateUpdate,qmax)+metric('Awake-map check',q.awakeCheck,qmax)+metric('PriorityQueue reinsert',q.reinsert,qmax)+metric('Sleep branch',q.sleepBranch,qmax)+metric('Future-head stop',q.futureStop,qmax));const c=tm.controls||{};adv+=section('Controls (nested)',metric('Sleep',c.sleep,max)+metric('Wake',c.wake,max)+metric('Alert',c.alert,max));const mods=tm.modulation||{};adv+=`<section><h2>Tick modulation</h2><div class="mods">${Object.entries(mods).map(([k,v])=>`<div class="mod"><span class="muted">${esc(k)}</span><strong>${calls(v.perTick)}</strong><span class="muted">${n(v.count).toLocaleString()} calls</span></div>`).join('')}</div></section>`}html+=`<details class="advanced"><summary>Advanced diagnostics · Tick Manager / queue / Level dispatch</summary><div class="advanced-body">${adv}</div></details>`;root.innerHTML=html}
function applyDeviceFilters(sync=true){if(sync){deviceState.q=document.getElementById('deviceSearch').value.trim().toLowerCase();deviceState.dimension=document.getElementById('deviceDimension').value;deviceState.min=Math.max(0,n(document.getElementById('deviceMin').value));deviceState.sort=document.getElementById('deviceSort').value;deviceState.top=parseInt(document.getElementById('deviceTop').value||'0',10)||0}let arr=allDevices.filter(d=>{if(deviceState.dimension!=='*'&&(d.dimension||'unknown')!==deviceState.dimension)return false;if(n(d.metric?.usPerTick)<deviceState.min)return false;if(deviceState.q){const hay=`${d.type} ${d.gridLabel||''} ${d.dimension} ${posText(d.position)}`.toLowerCase();if(!hay.includes(deviceState.q))return false}return true});const matched=arr.length;arr.sort((a,b)=>deviceState.sort==='priority'?devicePriorityScore(b)-devicePriorityScore(a):deviceState.sort==='avg'?avgCallUs(b.metric)-avgCallUs(a.metric):deviceState.sort==='calls'?n(b.metric?.callsPerTick)-n(a.metric?.callsPerTick):deviceState.sort==='p99'?spikeVal(b,'p99Us')-spikeVal(a,'p99Us'):deviceState.sort==='max'?spikeVal(b,'maxUs')-spikeVal(a,'maxUs'):deviceState.sort==='type'?String(a.type).localeCompare(String(b.type)):n(b.metric?.usPerTick)-n(a.metric?.usPerTick));if(deviceState.top>0)arr=arr.slice(0,deviceState.top);deviceFiltered=arr;document.getElementById('deviceStatus').innerHTML=`<span>Показано <strong>${arr.length}</strong> из <strong>${allDevices.length}</strong> detailed devices</span><span>Observed globally: <strong>${observedPhysicalCount()}</strong></span><span>Фильтру соответствуют: ${matched}</span><span>Load ≥ ${deviceState.min} µs/t</span><span>Linked in shown set: ${arr.filter(d=>d.gridLabel).length}</span>`;renderTypeSummary();document.getElementById('deviceList').innerHTML=arr.length?deviceRows(arr):'<div class="panel empty">Нет physical devices по текущему фильтру.</div>';saveUiState()}
function renderTypeSummary(){const map=new Map();for(const d of allDevices){const k=d.type||'unknown';let x=map.get(k);if(!x){x={type:k,count:0,total:0,max:0,calls:0};map.set(k,x)}const v=n(d.metric?.usPerTick);x.count++;x.total+=v;x.max=Math.max(x.max,v);x.calls+=n(d.metric?.callsPerTick)}const top=[...map.values()].sort((a,b)=>b.total-a.total).slice(0,15);document.getElementById('typeSummary').innerHTML=top.length?`<div class="type-grid">${top.map(x=>`<div class="type-card"><small>${esc(x.type)}</small><strong>${fmt(x.total)}</strong><small>${x.count} positions · max ${fmt(x.max)} · ${calls(x.calls)}</small></div>`).join('')}</div>`:''}
const reportTotal=(r,kind)=>{const key=kind==='physical'?'physicalUsPerTick':'gridCoreUsPerTick',gv=Number(r?.globalTotals?.[key]);if(Number.isFinite(gv))return gv;return kind==='physical'?[...(r?.physicalDevices||[])].reduce((a,d)=>a+n(d.metric?.usPerTick),0):[...(r?.grids||[])].reduce((a,g)=>a+n(g.metrics?.gridCore?.usPerTick),0)};
const gridTotalScope=r=>r?.globalTotals?.gridTotalsScope||'full-profile';
const gridTotalsComparable=(a,b)=>{const as=gridTotalScope(a),bs=gridTotalScope(b);if(as!==bs)return false;if(as!=='scout-estimate')return true;return n(a?.globalTotals?.gridMetricTicks)===n(b?.globalTotals?.gridMetricTicks)};
function gridDeviceSets(reportObj){const map=new Map();for(const d of (reportObj?.physicalDevices||[])){if(!d.gridLabel)continue;let set=map.get(d.gridLabel);if(!set){set=new Set();map.set(d.gridLabel,set)}set.add(deviceKey(d))}return map}
""");
        html.append("""
function setIntersectionSize(a,b){let small=a,big=b;if(a.size>b.size){small=b;big=a}let n=0;for(const v of small)if(big.has(v))n++;return n}
function gridAnchorBuckets(grids){const map=new Map();for(const g of (grids||[])){const k=gridKey(g);let arr=map.get(k);if(!arr){arr=[];map.set(k,arr)}arr.push(g)}return map}
function duplicateAnchorGroups(grids){let n=0;for(const arr of gridAnchorBuckets(grids).values())if(arr.length>1)n++;return n}
function fingerprintCompare(a,b){return b.dice-a.dice||b.inter-a.inter||b.coverage-a.coverage}
function fingerprintAmbiguous(best,second){if(!second)return false;return Math.abs(best.dice-second.dice)<0.05&&best.inter===second.inter&&Math.abs(best.coverage-second.coverage)<0.05}
function gridDiffRows(currentReport,baselineReport){const current=currentReport?.grids||[],baseline=baselineReport?.grids||[],cb=gridAnchorBuckets(current),bb=gridAnchorBuckets(baseline),usedC=new Set(),usedB=new Set(),out=[];
 for(const [key,ca] of cb){const ba=bb.get(key);if(!ba||ca.length!==1||ba.length!==1)continue;const c=ca[0],b=ba[0];usedC.add(c);usedB.add(b);const cv=valueOf(c,'core'),bv=valueOf(b,'core'),delta=cv-bv,pct=bv>0?delta/bv*100:(cv>0?Infinity:0);out.push({key,cur:c,base:b,cv,bv,delta,pct,status:delta>0?'regression':delta<0?'improvement':'same',matchKind:'anchor',matchScore:1,sharedDevices:0})}
 const cs=gridDeviceSets(currentReport),bs=gridDeviceSets(baselineReport),candidates=[];for(const c of current){if(usedC.has(c))continue;const a=cs.get(c.label);if(!a||!a.size)continue;for(const b of baseline){if(usedB.has(b)||b.dimension!==c.dimension)continue;const z=bs.get(b.label);if(!z||!z.size)continue;const inter=setIntersectionSize(a,z);if(!inter)continue;const dice=2*inter/(a.size+z.size),coverage=inter/Math.min(a.size,z.size);if(dice>=0.80&&coverage>=0.80)candidates.push({c,b,inter,dice,coverage})}}
 candidates.sort(fingerprintCompare);const byC=new Map(),byB=new Map();for(const m of candidates){let ca=byC.get(m.c);if(!ca){ca=[];byC.set(m.c,ca)}ca.push(m);let ba=byB.get(m.b);if(!ba){ba=[];byB.set(m.b,ba)}ba.push(m)}for(const arr of byC.values())arr.sort(fingerprintCompare);for(const arr of byB.values())arr.sort(fingerprintCompare);
 for(const m of candidates){if(usedC.has(m.c)||usedB.has(m.b))continue;const ca=byC.get(m.c)||[],ba=byB.get(m.b)||[];if(ca[0]!==m||ba[0]!==m)continue;if(fingerprintAmbiguous(m,ca[1])||fingerprintAmbiguous(m,ba[1]))continue;usedC.add(m.c);usedB.add(m.b);const cv=valueOf(m.c,'core'),bv=valueOf(m.b,'core'),delta=cv-bv,pct=bv>0?delta/bv*100:(cv>0?Infinity:0);out.push({key:`fingerprint:${m.c.label}:${m.b.label}`,cur:m.c,base:m.b,cv,bv,delta,pct,status:delta>0?'regression':delta<0?'improvement':'same',matchKind:'devices',matchScore:m.dice,sharedDevices:m.inter})}
 for(const c of current)if(!usedC.has(c)){const cv=valueOf(c,'core');out.push({key:`new:${gridKey(c)}:${c.label}`,cur:c,base:null,cv,bv:0,delta:cv,pct:cv>0?Infinity:0,status:'new',matchKind:'none',matchScore:0,sharedDevices:0})}for(const b of baseline)if(!usedB.has(b)){const bv=valueOf(b,'core');out.push({key:`gone:${gridKey(b)}:${b.label}`,cur:null,base:b,cv:0,bv,delta:-bv,pct:-100,status:'gone',matchKind:'none',matchScore:0,sharedDevices:0})}return out}
function gridMatchText(x){if(x.matchKind==='anchor')return 'тот же уникальный anchor';if(x.matchKind==='devices')return `та же сеть по устройствам: ${(x.matchScore*100).toFixed(0)}%, совпало ${x.sharedDevices}`;return ''}
function diffRows(current,baseline,keyFn,valFn){const cm=new Map(current.map(x=>[keyFn(x),x])),bm=new Map(baseline.map(x=>[keyFn(x),x]));const out=[];for(const key of new Set([...cm.keys(),...bm.keys()])){const cur=cm.get(key)||null,base=bm.get(key)||null,cv=cur?valFn(cur):0,bv=base?valFn(base):0,delta=cv-bv,pct=bv>0?delta/bv*100:(cv>0?Infinity:0);out.push({key,cur,base,cv,bv,delta,pct,status:!base?'new':!cur?'gone':delta>0?'regression':delta<0?'improvement':'same'})}return out}
const deltaClass=x=>x.status==='new'?'delta-new':x.status==='gone'?'delta-gone':x.delta>0?'delta-up':x.delta<0?'delta-down':'';
const deltaText=v=>`${v>0?'+':''}${fmt(v)}`;
const pctChange=x=>x.status==='new'?'NEW':x.status==='gone'?'GONE':Number.isFinite(x.pct)?`${x.pct>0?'+':''}${x.pct.toFixed(Math.abs(x.pct)>=100?0:1)}%`:'from 0';
function filterDiff(arr){let out=arr.filter(x=>{if(compareState.kind==='regressions'&&!(x.status==='regression'||x.status==='new'))return false;if(compareState.kind==='improvements'&&x.status!=='improvement')return false;if(compareState.kind==='new'&&x.status!=='new')return false;if(compareState.kind==='gone'&&x.status!=='gone')return false;if(x.status!=='new'&&x.status!=='gone'&&Math.abs(x.pct)<compareState.minPct)return false;if(Math.abs(x.delta)<compareState.minAbs)return false;return true});out.sort((a,b)=>compareState.sort==='pct'?(Math.abs(Number.isFinite(b.pct)?b.pct:1e12)-Math.abs(Number.isFinite(a.pct)?a.pct:1e12)):compareState.sort==='current'?b.cv-a.cv:compareState.sort==='baseline'?b.bv-a.bv:b.delta-a.delta);if(compareState.top>0)out=out.slice(0,compareState.top);return out}
function gridCompareRows(rows){if(!rows.length)return '<div class="empty">Нет Grid, подходящих под фильтр.</div>';const body=rows.map(x=>{const g=x.cur||x.base,pos=g?.anchor,dim=g?.dimension||'unknown',labels=x.cur&&x.base&&x.cur.label!==x.base.label?`${esc(x.base.label)} → ${esc(x.cur.label)}`:esc(g?.label||'');return `<tr><td class="left"><span class="status-pill ${deltaClass(x)}">${x.status}</span> ${x.cur?`<button class="grid-link" data-open-grid="${esc(x.cur.label)}">${labels}</button>`:labels}<div class="muted">${esc(gridMatchText(x))}</div><div class="muted">${esc(gridRegressionInsight(x))}</div></td><td>${fmt(x.cv)}</td><td>${fmt(x.bv)}</td><td class="${deltaClass(x)}">${deltaText(x.delta)}</td><td class="${deltaClass(x)}">${pctChange(x)}</td><td class="left dim" title="${esc(dim)}">${esc(dimShort(dim))}</td><td>${esc(posText(pos))}</td><td>${coordActions(dim,pos)}</td></tr>`}).join('');return `<div class="table-scroll"><table class="data-table"><thead><tr><th>Grid</th><th>Current</th><th>Baseline</th><th>Δ Core</th><th>Change</th><th>Dimension</th><th>Anchor</th><th>Actions</th></tr></thead><tbody>${body}</tbody></table></div>`}
function deviceCompareRows(rows){if(!rows.length)return '<div class="empty">Нет устройств, подходящих под фильтр.</div>';const body=rows.map(x=>{const d=x.cur||x.base,pos=d?.position,dim=d?.dimension||'unknown';return `<tr><td class="left clip"><span class="status-pill ${deltaClass(x)}">${x.status}</span> <b>${esc(d?.type||'unknown')}</b><div class="muted">${esc(deviceRegressionInsight(x))}</div></td><td>${fmt(x.cv)}</td><td>${fmt(x.bv)}</td><td class="${deltaClass(x)}">${deltaText(x.delta)}</td><td class="${deltaClass(x)}">${pctChange(x)}</td><td>${x.cur?gridLink(x.cur.gridLabel):'<span class="muted">—</span>'}</td><td class="left dim" title="${esc(dim)}">${esc(dimShort(dim))}</td><td>${esc(posText(pos))}</td><td>${coordActions(dim,pos)}</td></tr>`}).join('');return `<div class="table-scroll"><table class="data-table"><thead><tr><th>Device</th><th>Current</th><th>Baseline</th><th>Δ Load</th><th>Change</th><th>Grid</th><th>Dimension</th><th>Position</th><th>Actions</th></tr></thead><tbody>${body}</tbody></table></div>`}
function renderCompare(){if(!baselineReport){document.getElementById('compareMeta').innerHTML='<span>Baseline не выбран.</span>';document.getElementById('compareCards').innerHTML='';document.getElementById('compareInsights').innerHTML='<div class="empty">Выбери baseline JSON.</div>';document.getElementById('compareGridList').innerHTML='<div class="empty">Выбери baseline JSON.</div>';document.getElementById('compareDeviceList').innerHTML='<div class="empty">Выбери baseline JSON.</div>';return}const gd=gridDiffRows(report,baselineReport),dd=diffRows(allDevices,baselineReport.physicalDevices||[],deviceKey,d=>n(d.metric?.usPerTick));const gf=filterDiff(gd),df=filterDiff(dd);const cg=reportTotal(report,'grid'),bg=reportTotal(baselineReport,'grid'),cp=reportTotal(report,'physical'),bp=reportTotal(baselineReport,'physical');const gdlt=cg-bg,pdlt=cp-bp,fingerprint=gd.filter(x=>x.matchKind==='devices').length,samplingCompare=(report.sampling?.largeServerMode||baselineReport.sampling?.largeServerMode)?'<span class=\"plain-warn\">Grid/service side includes Large Server weighted estimates; physical device timings remain exact.</span>':'',scopeCompare=gridTotalsComparable(report,baselineReport)?'':`<span class=\"plain-warn\">Grid Core global scopes differ (${esc(gridTotalScope(report))} vs ${esc(gridTotalScope(baselineReport))}); global Grid Δ is not apples-to-apples. Physical Δ remains comparable.</span>`;renderRegressionInsights(gd,dd);document.getElementById('compareMeta').innerHTML=`${samplingCompare}${scopeCompare}<span>Current: <b>${esc(report.generatedAt||'this report')}</b></span><span>Baseline: <b>${esc(baselineName||baselineReport.generatedAt||'selected JSON')}</b></span><span>Grid match: unique anchor first; changed/duplicate anchors → unambiguous ≥80% physical-device fingerprint</span><span>Duplicate anchor groups: <b>${duplicateAnchorGroups(allGrids)}</b> current / <b>${duplicateAnchorGroups(baselineReport.grids||[])}</b> baseline</span><span>Fingerprint fallback matched: <b>${fingerprint}</b></span><span>Devices: dimension + type + position</span>`;document.getElementById('compareCards').innerHTML=`<div class="card"><small>Grid Core Current</small><b>${fmt(cg)}</b></div><div class="card"><small>Grid Core Δ</small><b class="${gridTotalsComparable(report,baselineReport)?(gdlt>0?'delta-up':gdlt<0?'delta-down':''):'plain-warn'}">${gridTotalsComparable(report,baselineReport)?deltaText(gdlt):'N/A · scope differs'}</b></div><div class="card"><small>Physical Current</small><b>${fmt(cp)}</b></div><div class="card"><small>Physical Δ</small><b class="${pdlt>0?'delta-up':pdlt<0?'delta-down':''}">${deltaText(pdlt)}</b></div><div class="card"><small>New / Gone</small><b>${gd.filter(x=>x.status==='new').length+dd.filter(x=>x.status==='new').length} / ${gd.filter(x=>x.status==='gone').length+dd.filter(x=>x.status==='gone').length}</b></div>`;document.getElementById('compareGridList').innerHTML=gridCompareRows(gf);document.getElementById('compareDeviceList').innerHTML=deviceCompareRows(df)}
function renderSimpleCompare(){const meta=document.getElementById('simpleCompareMeta'),box=document.getElementById('simpleCompare');if(!baselineReport){meta.innerHTML='<span>Прошлый профиль не выбран.</span>';box.innerHTML='';return}const gd=gridDiffRows(report,baselineReport),dd=diffRows(allDevices,baselineReport.physicalDevices||[],deviceKey,d=>n(d.metric?.usPerTick)),cg=reportTotal(report,'grid'),bg=reportTotal(baselineReport,'grid'),delta=cg-bg,pct=bg>0?delta/bg*100:0,gridComparable=gridTotalsComparable(report,baselineReport);const samplingCompare=(report.sampling?.largeServerMode||baselineReport.sampling?.largeServerMode)?'<span class=\"plain-warn\">Grid side содержит weighted estimates Large Server mode.</span>':'';meta.innerHTML=`${samplingCompare}<span>Сравниваем с: <b>${esc(baselineName||baselineReport.generatedAt||'baseline')}</b></span><span>Grid identity fallback: <b>${gd.filter(x=>x.matchKind==='devices').length}</b></span><span>Повторяющиеся anchors: <b>${duplicateAnchorGroups(allGrids)}</b> / <b>${duplicateAnchorGroups(baselineReport.grids||[])}</b></span>`;const worseG=gd.filter(x=>x.status==='regression').sort((a,b)=>b.delta-a.delta).slice(0,5),betterG=gd.filter(x=>x.status==='improvement').sort((a,b)=>a.delta-b.delta).slice(0,3),worseD=dd.filter(x=>x.status==='regression').sort((a,b)=>b.delta-a.delta).slice(0,5);const summary=!gridComparable?'Глобальный Grid Core нельзя корректно сравнить: профили используют разный scope (full-profile/scout-estimate). Physical devices и совпавшие detailed grids сравнивать можно.':delta>0?`AE2 в этом профиле тяжелее на ${fmt(Math.abs(delta))} (${pct.toFixed(1)}%).`:delta<0?`AE2 в этом профиле легче на ${fmt(Math.abs(delta))} (${Math.abs(pct).toFixed(1)}%).`:'Суммарная AE2-нагрузка почти не изменилась.';const gridItems=worseG.map(x=>{const g=x.cur||x.base;return `<div class="simple-issue"><h3>Сеть стала тяжелее · ${esc(g?.label||'')}</h3><div class="simple-load delta-up">+${fmt(x.delta)}</div><div class="simple-why">Было ${fmt(x.bv)}, стало ${fmt(x.cv)}. ${esc(gridMatchText(x))}<div class="muted" style="margin-top:6px">${esc(gridRegressionInsight(x))}</div></div><div class="simple-actions">${x.cur?coordActions(x.cur.dimension,x.cur.anchor):''}${x.cur?`<button data-open-grid="${esc(x.cur.label)}">Подробнее</button>`:''}</div></div>`}).join('');const deviceItems=worseD.map(x=>{const d=x.cur||x.base;return `<div class="simple-device"><div><b>${esc(humanType(d?.type))}</b><div class="where">${esc(dimShort(d?.dimension))} · ${esc(posText(d?.position))}</div></div><div><b class="delta-up">+${fmt(x.delta)}</b><div class="muted">${fmt(x.bv)} → ${fmt(x.cv)}</div></div><div>${x.cur&&x.base&&x.delta>0?esc(deviceRegressionInsight(x)):x.cur?esc(deviceFrequencyText(x.cur)):''}</div><div class="simple-actions">${coordActions(d?.dimension,d?.position)}${x.cur?.gridLabel&&gridByLabel(x.cur.gridLabel)?`<button data-open-grid="${esc(x.cur.gridLabel)}">Открыть сеть</button>`:''}</div></div>`}).join('');const improved=betterG.length?`<div class="muted" style="margin-top:10px">Стало легче: ${betterG.map(x=>`${esc((x.cur||x.base)?.label)} ${deltaText(x.delta)}`).join(' · ')}</div>`:'';box.innerHTML=`<div class="simple-compare-summary ${delta>0?'delta-up':delta<0?'delta-down':''}"><b>${summary}</b></div>${gridItems?`<h3>Что сильнее всего ухудшилось</h3><div class="simple-issues">${gridItems}</div>`:'<div class="plain-good">Сильных регрессий Grid не найдено.</div>'}${deviceItems?`<h3 style="margin-top:14px">Какие устройства стали тяжелее</h3><div class="simple-list">${deviceItems}</div>`:''}${improved}`}
function syncCompareState(){compareState.kind=document.getElementById('compareKind').value;compareState.sort=document.getElementById('compareSort').value;compareState.minPct=Math.max(0,n(document.getElementById('comparePct').value));compareState.minAbs=Math.max(0,n(document.getElementById('compareAbs').value));compareState.top=parseInt(document.getElementById('compareTop').value||'0',10)||0;saveUiState();renderCompare()}
async function loadBaselineFile(file,openExpert=false){if(!file)return;try{const text=await file.text();const parsed=JSON.parse(text);if(!parsed||!Array.isArray(parsed.grids))throw new Error('JSON does not contain grids[]');baselineReport=parsed;baselineName=file.name||'';renderSimpleCompare();if(openExpert){setSiteMode('expert',false);switchView('compare')}else renderSimple()}catch(e){baselineReport=null;baselineName='';document.getElementById('compareMeta').innerHTML=`<span class="delta-up">Не удалось открыть baseline: ${esc(e.message||e)}</span>`;document.getElementById('simpleCompareMeta').innerHTML=`<span class="delta-up">Не удалось открыть baseline: ${esc(e.message||e)}</span>`;renderCompare();renderSimpleCompare()}}
function copySummary(){if(siteMode==='simple')return adminSummaryText();const s=globalStats();if(currentView==='grids'&&selectedGrid){const g=selectedGrid;return `${g.label}: Core ${fmt(valueOf(g,'core'))}, Devices ${fmt(valueOf(g,'devices'))}, Scheduler ${fmt(valueOf(g,'scheduler'))}, Remainder ${fmt(valueOf(g,'overhead'))}, ${g.dimension} @ ${anchorText(g)}`}return `AE2 Overview: ${s.grids} grids, Grid Core ${fmt(s.core)}${globalTotals.gridTotalsScope==='scout-estimate'?' (scout estimate)':''}, Grid Devices ${fmt(s.devices)}, Scheduler ${fmt(s.scheduler)}, ${s.dims} dimensions, ${observedPhysicalCount()} physical devices`}

const gridDims=[...new Set(allGrids.map(g=>g.dimension||'unknown'))].sort((a,b)=>a.localeCompare(b));const deviceDims=[...new Set(allDevices.map(d=>d.dimension||'unknown'))].sort((a,b)=>a.localeCompare(b));populateDimensionSelect('gridDimension',gridDims);populateDimensionSelect('deviceDimension',deviceDims);
restoreUiState();syncStateToControls();
const samp=report.sampling||{},driveCov=report.driveCoverage||{},gs=globalStats(),obsPhys=observedPhysicalCount(),expGrids=n(reportLimits.exportedGrids)||allGrids.length,obsGrids=n(reportLimits.observedGrids)||gs.grids,expPhys=n(reportLimits.exportedPhysicalDevices)||allDevices.length,omittedGrids=Math.max(0,n(reportLimits.omittedGrids)),omittedPhys=Math.max(0,n(reportLimits.omittedPhysicalDevices));document.getElementById('reportMeta').innerHTML=`<span>Профиль: ${n(report.profileTicks).toLocaleString()} ticks${n(report.profileWallClockMs)>0?` · ${(n(report.profileWallClockMs)/1000).toFixed(1)}s · ~${n(report.observedProfileTps).toFixed(1)} TPS`:``}</span><span>Grid detail: <b>${expGrids.toLocaleString()}/${obsGrids.toLocaleString()}</b></span><span>Physical detail: <b>${expPhys.toLocaleString()}/${obsPhys.toLocaleString()}</b></span><span>Linked: ${gs.linked.toLocaleString()}/${obsPhys.toLocaleString()}</span><span>Dimensions: ${gs.dims.toLocaleString()}</span>${omittedGrids||omittedPhys?`<span class=\"plain-warn\">Compact report · omitted detail: ${omittedGrids.toLocaleString()} grids / ${omittedPhys.toLocaleString()} devices</span>`:''}${Object.keys(driveCov).length?`<span>ME Drive: <b>${n(driveCov.activeDriveTargets)}/${n(driveCov.physicalDriveTargets)}</b> active · unresolved ops <b>${n(driveCov.operations?.unresolvedBegins).toLocaleString()}</b></span>`:''}${samp.largeServerMode?`<span class=\"plain-warn\">Large Server mode · ${n(samp.levelLifecycleCallbacksSeenPerProfileTick).toFixed(0)} Level callbacks/t · ~1/${n(samp.effectiveSampleFactor).toFixed(1)} sampled · max shard ${n(samp.maxSampleFactor)}x${n(samp.skippedByBudget)>0?` · budget-skip ${n(samp.skippedByBudget).toLocaleString()}`:''}</span>`:`<span class=\"plain-good\">Exact Grid lifecycle mode</span>`}${runtimeSelection.selectionFrozen?`<span class=\"plain-good\">Runtime Top-N: <b>${n(runtimeSelection.selectedGrids)}</b> after ${n(runtimeSelection.scoutTicks)} scout ticks · kept ${n(runtimeSelection.gridLifecycleCallbacksKeptAfterCap).toLocaleString()} / skipped ${n(runtimeSelection.gridLifecycleCallbacksSkippedByCap).toLocaleString()} Grid callbacks after cap</span>`:''}${globalTotals.gridTotalsScope==='scout-estimate'?`<span class=\"plain-warn\">Global Grid totals = ${n(globalTotals.gridMetricTicks)}-tick scout estimate; physical total = full profile</span>`:''}<span>${esc(report.generatedAt||'')}</span>`;
document.getElementById('simpleModeBtn').onclick=()=>setSiteMode('simple');document.getElementById('expertModeBtn').onclick=()=>setSiteMode('expert');document.querySelectorAll('.nav-tabs button').forEach(b=>b.onclick=()=>switchView(b.dataset.view));
document.getElementById('jsonBtn').onclick=()=>{location.href=location.href.replace(/\\.html(?:[?#].*)?$/i,'.json')};document.getElementById('copyBtn').onclick=async()=>{const txt=copySummary();try{await navigator.clipboard.writeText(txt);document.getElementById('copyBtn').textContent='Скопировано';setTimeout(()=>document.getElementById('copyBtn').textContent='Копировать сводку',1200)}catch(e){prompt('Скопируй сводку:',txt)}};
['gridSearch','gridDimension','gridMin','gridSort','gridTop','gridActive'].forEach(id=>{const el=document.getElementById(id);el.addEventListener(id==='gridSearch'||id==='gridMin'?'input':'change',()=>applyGridFilters())});document.getElementById('gridShowAll').onclick=()=>{document.getElementById('gridSearch').value='';document.getElementById('gridDimension').value='*';document.getElementById('gridMin').value='0';document.getElementById('gridSort').value='core';document.getElementById('gridTop').value='0';document.getElementById('gridActive').checked=false;applyGridFilters()};
['deviceSearch','deviceDimension','deviceMin','deviceSort','deviceTop'].forEach(id=>{const el=document.getElementById(id);el.addEventListener(id==='deviceSearch'||id==='deviceMin'?'input':'change',()=>applyDeviceFilters())});document.getElementById('deviceShowAll').onclick=()=>{document.getElementById('deviceSearch').value='';document.getElementById('deviceDimension').value='*';document.getElementById('deviceMin').value='0';document.getElementById('deviceSort').value='time';document.getElementById('deviceTop').value='0';applyDeviceFilters()};
['compareKind','compareSort','comparePct','compareAbs','compareTop'].forEach(id=>{const el=document.getElementById(id);el.addEventListener(id==='comparePct'||id==='compareAbs'?'input':'change',syncCompareState)});document.getElementById('compareFile').addEventListener('change',e=>loadBaselineFile(e.target.files?.[0],true));document.getElementById('simpleCompareFile').addEventListener('change',e=>loadBaselineFile(e.target.files?.[0],false));const drop=document.getElementById('compareDrop');['dragenter','dragover'].forEach(ev=>drop.addEventListener(ev,e=>{e.preventDefault();drop.classList.add('drag')}));['dragleave','drop'].forEach(ev=>drop.addEventListener(ev,e=>{e.preventDefault();drop.classList.remove('drag')}));drop.addEventListener('drop',e=>loadBaselineFile(e.dataTransfer?.files?.[0],true));
async function copyText(text,button){try{await navigator.clipboard.writeText(text);if(button){const old=button.textContent;button.textContent='Copied';setTimeout(()=>button.textContent=old,900)}}catch(e){prompt('Скопируй:',text)}}
function openGrid(label){const g=gridByLabel(label);if(!g)return;setSiteMode('expert',false);document.getElementById('gridSearch').value=label;document.getElementById('gridDimension').value='*';document.getElementById('gridMin').value='0';document.getElementById('gridTop').value='0';document.getElementById('gridActive').checked=false;selectedGrid=g;switchView('grids');applyGridFilters(true);selectedGrid=gridByLabel(label)||selectedGrid;renderGridList();renderSelection();renderGridDetail()}
document.addEventListener('click',e=>{const copy=e.target.closest('[data-copy-text]');if(copy){e.preventDefault();e.stopPropagation();copyText(copy.getAttribute('data-copy-text')||'',copy);return}const gl=e.target.closest('[data-open-grid]');if(gl){e.preventDefault();e.stopPropagation();openGrid(gl.getAttribute('data-open-grid'));}});
renderOverview();applyGridFilters(false);applyDeviceFilters(false);setSiteMode(siteMode,false);
</script>
</body>
</html>

""");

        return html.toString().replace("__REPORT_JSON__", safeJson);
    }



    /**
     * Removes diagnostic-only AE2 Grid virtual blocks after the complete upload
     * snapshot has already been created, then adds one inclusive total marker
     * per grid for the in-game overlay.
     */
    public static void prepareClientOverlay() {
        Profiler profiler = Observable.INSTANCE.getPROFILER();

        for (Map<BlockPos, Profiler.TimingData> dimension : profiler.getBlockTimingsMap().values()) {
            List<BlockPos> remove = new ArrayList<>();
            for (Map.Entry<BlockPos, Profiler.TimingData> entry : dimension.entrySet()) {
                Profiler.TimingData data = entry.getValue();
                String name = data == null ? null : data.getName();
                if (name != null && name.startsWith("AE2 Grid #")) {
                    remove.add(entry.getKey());
                }
            }
            for (BlockPos pos : remove) {
                dimension.remove(pos);
            }
        }

        for (GridSnapshot snapshot : reportGridSnapshots(snapshotsSorted(), requestedReportGridLimit())) {
            if (snapshot.gridCoreNanos <= 0L || snapshot.gridCoreCalls <= 0
                    || snapshot.anchorEntity == null || snapshot.virtualBase == null
                    || !(snapshot.anchorEntity.getLevel() instanceof ServerLevel)) {
                continue;
            }
            publishMetric(profiler, snapshot.anchorEntity.getLevel(), snapshot.virtualBase, 0,
                    snapshot.label + " / Total (inclusive)", snapshot.gridCoreNanos, snapshot.gridCoreCalls);
        }
    }

    private static void appendDriveCoverageJson(StringBuilder out, DriveCoverageReportSnapshot coverage) {
        CompatTiming.DriveCoverageSnapshot resolver = coverage.resolver;
        indent(out, 1).append("\"driveCoverage\": {\n");
        indent(out, 2).append("\"registeredDrives\": ").append(resolver.registeredDrives).append(",\n");
        indent(out, 2).append("\"physicalDriveTargets\": ").append(coverage.physicalDriveTargets).append(",\n");
        indent(out, 2).append("\"activeDriveTargets\": ").append(coverage.activeDriveTargets).append(",\n");
        indent(out, 2).append("\"inactiveDriveTargets\": ").append(Math.max(0, coverage.physicalDriveTargets - coverage.activeDriveTargets)).append(",\n");
        indent(out, 2).append("\"resolver\": {")
                .append("\"resolutionAttempts\": ").append(resolver.resolutionAttempts).append(", ")
                .append("\"fallbackResolutionAttempts\": ").append(Math.max(0L, resolver.resolutionAttempts - resolver.resolvedRecordedOwner - resolver.resolvedStorageMountOwner)).append(", ")
                .append("\"resolvedWatchers\": ").append(resolver.resolvedWatchers).append(", ")
                .append("\"unresolvedWatchers\": ").append(resolver.unresolvedWatchers).append(", ")
                .append("\"unresolvedWatcherIdentities\": ").append(resolver.unresolvedWatchers).append(", ")
                .append("\"uniqueResolvedDrives\": ").append(resolver.uniqueResolvedDrives).append(", ")
                .append("\"resolvedByDirectOwner\": ").append(resolver.resolvedDirect).append(", ")
                .append("\"resolvedByCallbackCapture\": ").append(resolver.resolvedCallback).append(", ")
                .append("\"resolvedByRecordedOwner\": ").append(resolver.resolvedRecordedOwner).append(", ")
                .append("\"resolvedByRecordedSnapshot\": ").append(resolver.resolvedRecordedSnapshot).append(", ")
                .append("\"resolvedByStorageMountOwner\": ").append(resolver.resolvedStorageMountOwner).append(", ")
                .append("\"hotPathSessionCache\": true, ")
                .append("\"hotPathAllocationFreeStack\": true, ")
                .append("\"hotPathTimingStateInitializations\": ").append(resolver.hotPathTimingStateInitializations).append(", ")
                .append("\"sessionPhysicalPreparations\": ").append(resolver.sessionPhysicalPreparations).append(", ")
                .append("\"storageMountOwnerEntries\": ").append(resolver.storageMountOwnerEntries).append(", ")
                .append("\"storageMountOwnerEntriesAtSessionStart\": ").append(resolver.storageMountOwnerEntriesAtSessionStart).append(", ")
                .append("\"storageProviderMountPasses\": ").append(resolver.storageProviderMountPasses).append(", ")
                .append("\"storageMountWatchersSeen\": ").append(resolver.storageMountWatchersSeen).append(", ")
                .append("\"storageMountAccessFailures\": ").append(resolver.storageMountAccessFailures).append(", ")
                .append("\"mountProviderIdentityEntries\": ").append(resolver.mountProviderIdentityEntries).append(", ")
                .append("\"passiveOwnerHistory\": true, ")
                .append("\"recordedOwnerEntries\": ").append(resolver.recordedOwnerEntries).append(", ")
                .append("\"recordedOwnerEntriesAtSessionStart\": ").append(resolver.recordedOwnerEntriesAtSessionStart).append(", ")
                .append("\"constructorOwnerCaptures\": ").append(resolver.constructorOwnerCaptures).append(", ")
                .append("\"recordedOwnerUpdates\": ").append(resolver.recordedOwnerUpdates).append(", ")
                .append("\"recordedOwnerAccessFailures\": ").append(resolver.recordedOwnerAccessFailures).append(", ")
                .append("\"resolvedByHostMapping\": ").append(resolver.resolvedHostMapping).append(", ")
                .append("\"preMappedWatchers\": ").append(resolver.resolvedHostMapping).append(", ")
                .append("\"hostMappingScans\": ").append(resolver.hostMappingScans).append(", ")
                .append("\"hostMappingWatchersSeen\": ").append(resolver.hostMappingWatchersSeen).append(", ")
                .append("\"hostMappingAccessFailures\": ").append(resolver.hostMappingAccessFailures).append(", ")
                .append("\"stableCellResolutionAttempts\": ").append(resolver.stableCellResolutionAttempts).append(", ")
                .append("\"resolvedByCellDelegateIdentity\": ").append(resolver.resolvedCellDelegateIdentity).append(", ")
                .append("\"resolvedByCellStackIdentity\": ").append(resolver.resolvedCellStackIdentity).append(", ")
                .append("\"resolvedByCellUuid\": ").append(resolver.resolvedCellUuid).append(", ")
                .append("\"cellDelegateOwnerEntries\": ").append(resolver.cellDelegateOwnerEntries).append(", ")
                .append("\"cellStackOwnerEntries\": ").append(resolver.cellStackOwnerEntries).append(", ")
                .append("\"cellUuidOwnerEntries\": ").append(resolver.cellUuidOwnerEntries).append(", ")
                .append("\"typedCellResolutionAttempts\": ").append(resolver.cellResolutionAttempts).append(", ")
                .append("\"resolvedByCellSaveProvider\": ").append(resolver.resolvedCellSaveProvider).append(", ")
                .append("\"cellDelegateTypeBasic\": ").append(resolver.cellDelegateBasic).append(", ")
                .append("\"cellDelegateTypeOther\": ").append(resolver.cellDelegateOther).append(", ")
                .append("\"cellDelegateNull\": ").append(resolver.cellDelegateNull).append(", ")
                .append("\"cellDelegateAccessFailures\": ").append(resolver.cellDelegateAccessFailures).append(", ")
                .append("\"cellSaveProviderAccessFailures\": ").append(resolver.cellSaveProviderAccessFailures).append(", ")
                .append("\"cellSaveProviderCaptureMisses\": ").append(resolver.cellSaveProviderCaptureMisses).append(", ")
                .append("\"legacyLazyRefreshDisabled\": true, ")
                .append("\"unresolvedWithoutDirectAccessors\": ").append(resolver.unresolvedWithoutDirectAccessors).append(", ")
                .append("\"unresolvedWithoutCallbackAccessors\": ").append(resolver.unresolvedWithoutCallbackAccessors).append(", ")
                .append("\"unresolvedReasons\": {")
                .append("\"noOwnerAccessors\": ").append(resolver.unresolvedNoOwnerAccessors).append(", ")
                .append("\"noCapturedDrive\": ").append(resolver.unresolvedNoCapturedDrive).append(", ")
                .append("\"safetyReject\": ").append(resolver.unresolvedSafetyReject).append(", ")
                .append("\"accessFailure\": ").append(resolver.unresolvedAccessFailure).append(", ")
                .append("\"directOwnerNotDrive\": ").append(resolver.unresolvedDirectOwnerNotDrive).append(", ")
                .append("\"other\": ").append(resolver.unresolvedOther).append("}, ")
                .append("\"unresolvedMountProviderTypes\": [");
        int mountProviderTypeIndex = 0;
        for (Map.Entry<String, Long> entry : resolver.unresolvedMountProviderTypes.entrySet()) {
            if (mountProviderTypeIndex++ > 0) {
                out.append(',');
            }
            out.append('{')
                    .append("\"type\": ").append(jsonQuote(entry.getKey())).append(", ")
                    .append("\"watchers\": ").append(entry.getValue())
                    .append('}');
        }
        out.append("], ")
                .append("\"unresolvedDelegateTypes\": [");
        int delegateTypeIndex = 0;
        for (Map.Entry<String, Long> entry : resolver.unresolvedDelegateTypes.entrySet()) {
            if (delegateTypeIndex++ > 0) {
                out.append(',');
            }
            out.append('{')
                    .append("\"type\": ").append(jsonQuote(entry.getKey())).append(", ")
                    .append("\"watchers\": ").append(entry.getValue())
                    .append('}');
        }
        out.append("]},\n");
        indent(out, 2).append("\"operations\": {")
                .append("\"begins\": ").append(resolver.operationBegins).append(", ")
                .append("\"resolvedBegins\": ").append(resolver.resolvedOperationBegins).append(", ")
                .append("\"unresolvedBegins\": ").append(resolver.unresolvedOperationBegins).append(", ")
                .append("\"nestedSameDriveBegins\": ").append(resolver.nestedSameDriveBegins).append(", ")
                .append("\"timedBegins\": ").append(resolver.timedOperationBegins).append(", ")
                .append("\"exactTimedCallsFromPhysical\": ").append(coverage.exactTimedCalls).append(", ")
                .append("\"beginExtract\": ").append(resolver.beginExtract).append(", ")
                .append("\"beginInsert\": ").append(resolver.beginInsert).append(", ")
                .append("\"beginPreferred\": ").append(resolver.beginPreferred).append(", ")
                .append("\"beginAvailableStacks\": ").append(resolver.beginAvailableStacks).append(", ")
                .append("\"beginOther\": ").append(resolver.beginOther).append(", ")
                .append("\"timedExtract\": ").append(coverage.extractCalls).append(", ")
                .append("\"timedInsert\": ").append(coverage.insertCalls).append(", ")
                .append("\"timedPreferred\": ").append(coverage.preferredCalls).append(", ")
                .append("\"timedAvailableStacks\": ").append(coverage.availableStacksCalls).append(", ")
                .append("\"timedOther\": ").append(coverage.otherCalls).append("}\n");
        indent(out, 1).append("},\n");
    }

    private static String buildDetailedReportJson(
            List<GridSnapshot> allSnapshots, List<GridSnapshot> snapshots,
            List<PhysicalDeviceSnapshot> allPhysicalDevices, List<PhysicalDeviceSnapshot> physicalDevices,
            DriveCoverageReportSnapshot driveCoverage, int profileTicks, int gridLimit, int physicalDeviceLimit) {
        StringBuilder out = new StringBuilder(Math.max(4096, snapshots.size() * 4096));
        out.append("{\n");
        // format remains the data-schema compatibility ID so older v18 baselines keep working.
        jsonField(out, 1, "format", "observable-ae2-grid-v18", true);
        jsonNumberField(out, 1, "schemaVersion", 18, true);
        jsonField(out, 1, "profilerVersion", "20.5.3", true);
        jsonField(out, 1, "generatedAt", Instant.now().toString(), true);
        jsonNumberField(out, 1, "profileTicks", profileTicks, true);
        long profileWallClockMs = Math.max(1L, System.currentTimeMillis() - sessionStartedWallMillis);
        jsonNumberField(out, 1, "profileWallClockMs", profileWallClockMs, true);
        indent(out, 1).append("\"observedProfileTps\": ")
                .append(String.format(Locale.ROOT, "%.6f", profileTicks * 1000.0 / profileWallClockMs))
                .append(",\n");
        jsonField(out, 1, "note",
                "Inclusive/nested metrics must not be summed. Grid Core contains services; Tick Manager contains queue/devices. v20.5.3 keeps the proven v20.5.2/v20.4.1/v20.3.2.16 collection, Drive ownership, hot-path behavior and Admin UI unchanged, while adding an administration-only consistency guard. The Admin Report still detects large one-off physical outliers and estimates exported physical background after removing known top outliers for triage only, and now refuses confident TickManager/device attribution when remainder is a dominant majority of Grid Core or nested inclusive buckets contradict their measured parent hierarchy. These are report-derived heuristics over existing measurements, not new timing hooks or replacements for the raw metrics. All previous technical diagnostics remain available in Expert mode. No new AE2 injection points are added. AE2 profiling remains opt-in with the 4-tick scout Top-N runtime detail cap. Drive ownership remains authoritative through StorageService.ProviderState.mount() for both appeng DriveBlockEntity and ExtendedAE TileExDrive. Exact Drive call counts, elapsed storage time and spike recording remain enabled. profileWallClockMs and observedProfileTps describe the observed profiling window and are diagnostic rather than a baseline TPS measurement. Compact report limits remain in place. When runtime selection freezes, global Grid totals are a scout-window estimate; physical timing/spikes remain full-profile.", true);

        long globalGridCoreNanos = 0L;
        long globalServiceNanos = 0L;
        long globalDeviceNanos = 0L;
        long globalSchedulerNanos = 0L;
        Set<String> globalDimensions = new HashSet<>();
        for (GridSnapshot snapshot : allSnapshots) {
            globalGridCoreNanos += Math.max(0L, snapshot.gridCoreNanos);
            globalServiceNanos += Math.max(0L, snapshot.serviceTotalNanos);
            globalDeviceNanos += Math.max(0L, snapshot.deviceNanos);
            if (snapshot.tickManager != null) {
                globalSchedulerNanos += Math.max(0L, snapshot.tickManager.queueBookkeepingNanos);
            }
            globalDimensions.add(snapshotDimension(snapshot));
        }
        int globalGridMetricTicks = profileTicks;
        boolean globalGridTotalsFromScout = false;
        if (runtimeSelectionFrozen && runtimeScoutTicks > 0) {
            globalGridCoreNanos = runtimeScoutGridCoreNanos;
            globalServiceNanos = runtimeScoutServiceNanos;
            globalDeviceNanos = runtimeScoutDeviceNanos;
            globalSchedulerNanos = runtimeScoutSchedulerNanos;
            globalGridMetricTicks = runtimeScoutTicks;
            globalGridTotalsFromScout = true;
        }

        long globalPhysicalNanos = 0L;
        int globalLinkedPhysical = 0;
        for (PhysicalDeviceSnapshot device : allPhysicalDevices) {
            globalPhysicalNanos += Math.max(0L, device.nanos);
            if (device.gridLabel != null && !device.gridLabel.isBlank()) {
                globalLinkedPhysical++;
            }
        }

        indent(out, 1).append("\"diagnosticModel\": {")
                .append("\"version\": ").append(jsonQuote("20.5.3")).append(", ")
                .append("\"classificationScope\": ").append(jsonQuote("report-derived")).append(", ")
                .append("\"newInjectionPoints\": false, ")
                .append("\"physicalTiming\": ").append(jsonQuote("exact-full-profile")).append(", ")
                .append("\"gridDetailScope\": ").append(jsonQuote(runtimeSelectionFrozen ? "runtime-top-n-after-scout" : "full-profile")).append(", ")
                .append("\"foreignDispatchRole\": ").append(jsonQuote("secondary-signal")).append(", ")
                .append("\"physicalGridAggregateScope\": ").append(jsonQuote("exported-physical-lower-bound-when-compact")).append(", ")
                .append("\"burstDetection\": ").append(jsonQuote("call-count-aware-millisecond-stall")).append(", ")
                .append("\"adminReport\": ").append(jsonQuote("human-readable-triage-top5")).append(", ")
                .append("\"adminReportCollection\": ").append(jsonQuote("report-derived-no-new-hooks")).append(", ")
                 .append("\"adminUiHotfix\": ").append(jsonQuote("escaped-newline-js-initialization")).append(", ")
                .append("\"adminAccuracy\": ").append(jsonQuote("spike-adjusted-sustained-and-consistency-triage")).append(", ")
                .append("\"adminRemainderGuard\": ").append(jsonQuote("majority-or-strongly-dominant-remainder-overrides-attribution")).append(", ")
                .append("\"adminConsistencyGuard\": ").append(jsonQuote("inclusive-hierarchy-mismatch-downgrades-attribution")).append(", ")
                .append("\"adminPriorityModel\": ").append(jsonQuote("impact-ranked-cause-confidence-separated")).append(", ")
                .append("\"deviceOutlierModel\": ").append(jsonQuote("primary-pattern-plus-large-outlier"))
                .append("},\n");

        indent(out, 1).append("\"reportLimits\": {")
                .append("\"requestedGridLimit\": ").append(gridLimit).append(", ")
                .append("\"observedGrids\": ").append(allSnapshots.size()).append(", ")
                .append("\"exportedGrids\": ").append(snapshots.size()).append(", ")
                .append("\"omittedGrids\": ").append(Math.max(0, allSnapshots.size() - snapshots.size())).append(", ")
                .append("\"physicalDeviceLimit\": ").append(physicalDeviceLimit).append(", ")
                .append("\"observedPhysicalDevices\": ").append(allPhysicalDevices.size()).append(", ")
                .append("\"exportedPhysicalDevices\": ").append(physicalDevices.size()).append(", ")
                .append("\"omittedPhysicalDevices\": ").append(Math.max(0, allPhysicalDevices.size() - physicalDevices.size())).append(", ")
                .append("\"dispatchLevelsPerGrid\": ").append(REPORT_DISPATCH_LEVEL_LIMIT)
                .append("},\n");

        indent(out, 1).append("\"runtimeSelection\": {")
                .append("\"mode\": ").append(jsonQuote("scout-top-n-runtime-detail")).append(", ")
                .append("\"scoutTicksTarget\": ").append(RUNTIME_GRID_SCOUT_TICKS).append(", ")
                .append("\"scoutTicks\": ").append(runtimeScoutTicks).append(", ")
                .append("\"selectionFrozen\": ").append(runtimeSelectionFrozen).append(", ")
                .append("\"discoveredAtFreeze\": ").append(runtimeDiscoveredAtFreeze).append(", ")
                .append("\"selectedGrids\": ").append(runtimeSelectedGridCount).append(", ")
                .append("\"gridLifecycleCallbacksSkippedByCap\": ").append(runtimeGridLifecycleSkippedByCap).append(", ")
                .append("\"gridLifecycleCallbacksKeptAfterCap\": ").append(runtimeGridLifecycleKeptAfterCap).append(", ")
                .append("\"globalGridTotalsScope\": ").append(jsonQuote(globalGridTotalsFromScout ? "scout-estimate" : "full-profile")).append(", ")
                .append("\"physicalTimingScope\": ").append(jsonQuote("all-observed-full-profile"))
                .append("},\n");

        double gridNanosToUsPerTick = globalGridMetricTicks <= 0 ? 0.0 : 1.0 / (1000.0 * globalGridMetricTicks);
        double physicalNanosToUsPerTick = profileTicks <= 0 ? 0.0 : 1.0 / (1000.0 * profileTicks);
        indent(out, 1).append("\"globalTotals\": {")
                .append("\"gridTotalsScope\": ").append(jsonQuote(globalGridTotalsFromScout ? "scout-estimate" : "full-profile")).append(", ")
                .append("\"gridMetricTicks\": ").append(globalGridMetricTicks).append(", ")
                .append("\"observedGrids\": ").append(allSnapshots.size()).append(", ")
                .append("\"observedDimensions\": ").append(globalDimensions.size()).append(", ")
                .append("\"observedPhysicalDevices\": ").append(allPhysicalDevices.size()).append(", ")
                .append("\"linkedPhysicalDevices\": ").append(globalLinkedPhysical).append(", ")
                .append("\"gridCoreUsPerTick\": ").append(decimal(globalGridCoreNanos * gridNanosToUsPerTick)).append(", ")
                .append("\"gridServicesUsPerTick\": ").append(decimal(globalServiceNanos * gridNanosToUsPerTick)).append(", ")
                .append("\"gridDevicesUsPerTick\": ").append(decimal(globalDeviceNanos * gridNanosToUsPerTick)).append(", ")
                .append("\"schedulerUsPerTick\": ").append(decimal(globalSchedulerNanos * gridNanosToUsPerTick)).append(", ")
                .append("\"physicalUsPerTick\": ").append(decimal(globalPhysicalNanos * physicalNanosToUsPerTick))
                .append("},\n");

        SamplingSnapshot sampling = SAMPLING.snapshot();
        indent(out, 1).append("\"sampling\": {")
                .append("\"largeServerMode\": ").append(sampling.largeServerMode).append(", ")
                .append("\"exactGridThreshold\": ").append(LARGE_SERVER_EXACT_GRID_THRESHOLD).append(", ")
                .append("\"targetDetailedCallbacksPerTick\": ").append(LARGE_SERVER_TARGET_DETAILED_CALLBACKS_PER_TICK).append(", ")
                .append("\"hardDetailedCallbackBudgetPerTick\": ").append(LARGE_SERVER_HARD_DETAILED_CALLBACK_BUDGET_PER_TICK).append(", ")
                .append("\"exactLifecycleCallbacks\": ").append(sampling.exactLifecycleCallbacks).append(", ")
                .append("\"levelLifecycleCallbacksSeen\": ").append(sampling.levelCallbacksSeen).append(", ")
                .append("\"levelLifecycleCallbacksSampled\": ").append(sampling.levelCallbacksSampled).append(", ")
                .append("\"skippedBySampling\": ").append(sampling.skippedBySampling).append(", ")
                .append("\"skippedByBudget\": ").append(sampling.skippedByBudget).append(", ")
                .append("\"maxSampleFactor\": ").append(sampling.maxSampleFactor).append(", ")
                .append("\"effectiveSampleFactor\": ").append(decimal(sampling.effectiveSampleFactor)).append(", ")
                .append("\"maxObservedCallbacksPerTick\": ").append(sampling.maxObservedCallbacksPerTick).append(", ")
                .append("\"maxDetailedCallbacksPerTick\": ").append(sampling.maxDetailedCallbacksPerTick).append(", ")
                .append("\"levelLifecycleCallbacksSeenPerProfileTick\": ").append(decimal(profileTicks <= 0 ? 0.0 : sampling.levelCallbacksSeen / (double) profileTicks)).append(", ")
                .append("\"levelLifecycleCallbacksSampledPerProfileTick\": ").append(decimal(profileTicks <= 0 ? 0.0 : sampling.levelCallbacksSampled / (double) profileTicks))
                .append("},\n");
        appendDriveCoverageJson(out, driveCoverage);
        indent(out, 1).append("\"physicalDevices\": [\n");
        for (int i = 0; i < physicalDevices.size(); i++) {
            PhysicalDeviceSnapshot device = physicalDevices.get(i);
            indent(out, 2).append("{\n");
            jsonField(out, 3, "dimension", device.dimension, true);
            indent(out, 3).append("\"position\": ");
            appendPosition(out, device.position);
            out.append(",\n");
            jsonField(out, 3, "type", device.type, true);
            if (device.gridLabel == null || device.gridLabel.isBlank()) {
                indent(out, 3).append("\"gridLabel\": null,\n");
            } else {
                jsonField(out, 3, "gridLabel", device.gridLabel, true);
            }
            jsonMetricField(out, 3, "metric", device.nanos, device.calls, profileTicks, true);
            appendPhysicalSpikeJson(out, 3, device.spike);
            out.append('\n');
            indent(out, 2).append('}');
            if (i + 1 < physicalDevices.size()) {
                out.append(',');
            }
            out.append('\n');
        }
        indent(out, 1).append("],\n");
        indent(out, 1).append("\"grids\": [\n");

        for (int i = 0; i < snapshots.size(); i++) {
            GridSnapshot snapshot = snapshots.get(i);
            indent(out, 2).append("{\n");
            jsonField(out, 3, "label", snapshot.label, true);
            String dimension = snapshotDimension(snapshot);
            jsonField(out, 3, "dimension", dimension, true);

            indent(out, 3).append("\"anchor\": ");
            appendPosition(out, snapshot.anchor);
            out.append(",\n");

            indent(out, 3).append("\"metrics\": {\n");
            jsonMetricField(out, 4, "gridCore", snapshot.gridCoreNanos, snapshot.gridCoreCalls, profileTicks, true);
            jsonMetricField(out, 4, "gridServices", snapshot.serviceTotalNanos, snapshot.serviceTotalCalls, profileTicks, true);
            jsonMetricField(out, 4, "gridOverhead", snapshot.gridOverheadNanos, snapshot.gridCoreCalls, profileTicks, true);
            jsonMetricField(out, 4, "devices", snapshot.deviceNanos, snapshot.deviceCalls, profileTicks, false);
            indent(out, 3).append("},\n");

            indent(out, 3).append("\"corePhases\": {\n");
            Phase[] phases = Phase.values();
            for (int p = 0; p < phases.length; p++) {
                Phase phase = phases[p];
                MetricSnapshot metric = snapshot.gridPhases.get(phase);
                if (metric == null) {
                    metric = new MetricSnapshot(0L, 0);
                }
                jsonMetricField(out, 4, phase.name().toLowerCase(Locale.ROOT),
                        metric.nanos, metric.calls, profileTicks, p + 1 < phases.length);
            }
            indent(out, 3).append("},\n");

            indent(out, 3).append("\"services\": [\n");
            for (int s = 0; s < snapshot.services.size(); s++) {
                ServiceSnapshot service = snapshot.services.get(s);
                indent(out, 4).append("{\n");
                jsonField(out, 5, "name", service.displayName, true);
                jsonField(out, 5, "className", service.className, true);
                jsonField(out, 5, "kind", service.known.name(), true);
                jsonMetricField(out, 5, "total", service.total.nanos, service.total.calls, profileTicks, true);
                indent(out, 5).append("\"phases\": {\n");
                for (int p = 0; p < phases.length; p++) {
                    Phase phase = phases[p];
                    MetricSnapshot metric = service.phases.get(phase);
                    if (metric == null) {
                        metric = new MetricSnapshot(0L, 0);
                    }
                    jsonMetricField(out, 6, phase.name().toLowerCase(Locale.ROOT),
                            metric.nanos, metric.calls, profileTicks, p + 1 < phases.length);
                }
                indent(out, 5).append("}\n");
                indent(out, 4).append("}");
                if (s + 1 < snapshot.services.size()) {
                    out.append(',');
                }
                out.append('\n');
            }
            indent(out, 3).append("],\n");

            indent(out, 3).append("\"tickManager\": ");
            appendTickManagerJson(out, snapshot.tickManager, profileTicks, 3);
            out.append('\n');

            indent(out, 2).append("}");
            if (i + 1 < snapshots.size()) {
                out.append(',');
            }
            out.append('\n');
        }

        indent(out, 1).append("]\n");
        out.append("}\n");
        return out.toString();
    }

    private static void appendTickManagerJson(StringBuilder out, TickManagerSnapshot tickManager,
                                              int profileTicks, int indentLevel) {
        if (tickManager == null) {
            out.append("null");
            return;
        }

        out.append("{\n");
        jsonMetricField(out, indentLevel + 1, "service", tickManager.serviceNanos,
                tickManager.serviceCalls, profileTicks, true);
        jsonMetricField(out, indentLevel + 1, "levelQueue", tickManager.levelQueue.nanos,
                tickManager.levelQueue.calls, profileTicks, true);
        jsonMetricField(out, indentLevel + 1, "queue", tickManager.queue.nanos,
                tickManager.queue.calls, profileTicks, true);
        jsonMetricField(out, indentLevel + 1, "schedulerRemainder", tickManager.queueBookkeepingNanos,
                tickManager.queue.calls, profileTicks, true);
        jsonMetricField(out, indentLevel + 1, "queueResidual", tickManager.queueResidualNanos,
                tickManager.queue.calls, profileTicks, true);
        jsonMetricField(out, indentLevel + 1, "levelDispatchOverhead", tickManager.levelDispatchNanos,
                tickManager.levelQueue.calls, profileTicks, true);
        jsonMetricField(out, indentLevel + 1, "outerOverhead", tickManager.outerOverheadNanos,
                tickManager.serviceCalls, profileTicks, true);
        jsonMetricField(out, indentLevel + 1, "devices", tickManager.deviceNanos,
                tickManager.deviceCalls, profileTicks, true);

        long dispatchLevelsAggregateNanos = 0L;
        int dispatchLevelsAggregateCalls = 0;
        long foreignDispatchAggregateNanos = 0L;
        int foreignDispatchAggregateCalls = 0;
        for (LevelDispatchSnapshot dispatch : tickManager.dispatchLevels) {
            dispatchLevelsAggregateNanos += dispatch.total.nanos;
            dispatchLevelsAggregateCalls += dispatch.total.calls;
            if (!dispatch.anchorDimension) {
                foreignDispatchAggregateNanos += dispatch.total.nanos;
                foreignDispatchAggregateCalls += dispatch.total.calls;
            }
        }
        jsonMetricField(out, indentLevel + 1, "dispatchLevelsAggregate", dispatchLevelsAggregateNanos,
                dispatchLevelsAggregateCalls, profileTicks, true);
        jsonMetricField(out, indentLevel + 1, "foreignDispatchAggregate", foreignDispatchAggregateNanos,
                foreignDispatchAggregateCalls, profileTicks, true);
        int dispatchLevelsTotal = tickManager.dispatchLevels.size();
        int dispatchLevelsExported = Math.min(dispatchLevelsTotal, REPORT_DISPATCH_LEVEL_LIMIT);
        indent(out, indentLevel + 1).append("\"dispatchLevelsTotal\": ").append(dispatchLevelsTotal).append(",\n");
        indent(out, indentLevel + 1).append("\"dispatchLevelsOmitted\": ")
                .append(Math.max(0, dispatchLevelsTotal - dispatchLevelsExported)).append(",\n");
        indent(out, indentLevel + 1).append("\"dispatchLevels\": [\n");
        for (int i = 0; i < dispatchLevelsExported; i++) {
            LevelDispatchSnapshot dispatch = tickManager.dispatchLevels.get(i);
            indent(out, indentLevel + 2).append("{\n");
            jsonField(out, indentLevel + 3, "dimension", dispatch.dimension, true);
            indent(out, indentLevel + 3).append("\"anchorDimension\": ")
                    .append(dispatch.anchorDimension).append(",\n");
            jsonMetricField(out, indentLevel + 3, "total", dispatch.total.nanos,
                    dispatch.total.calls, profileTicks, true);
            jsonMetricField(out, indentLevel + 3, "levelStart", dispatch.levelStart.nanos,
                    dispatch.levelStart.calls, profileTicks, true);
            jsonMetricField(out, indentLevel + 3, "levelEnd", dispatch.levelEnd.nanos,
                    dispatch.levelEnd.calls, profileTicks, false);
            indent(out, indentLevel + 2).append('}');
            if (i + 1 < dispatchLevelsExported) {
                out.append(',');
            }
            out.append('\n');
        }
        indent(out, indentLevel + 1).append("],\n");

        indent(out, indentLevel + 1).append("\"queuePhases\": {\n");
        jsonMetricField(out, indentLevel + 2, "headDueCheck", tickManager.headDueCheck.nanos,
                tickManager.headDueCheck.calls, profileTicks, true);
        jsonMetricField(out, indentLevel + 2, "dequeuePrep", tickManager.dequeuePrep.nanos,
                tickManager.dequeuePrep.calls, profileTicks, true);
        jsonMetricField(out, indentLevel + 2, "rateUpdate", tickManager.rateUpdate.nanos,
                tickManager.rateUpdate.calls, profileTicks, true);
        jsonMetricField(out, indentLevel + 2, "awakeCheck", tickManager.awakeCheck.nanos,
                tickManager.awakeCheck.calls, profileTicks, true);
        jsonMetricField(out, indentLevel + 2, "reinsert", tickManager.reinsert.nanos,
                tickManager.reinsert.calls, profileTicks, true);
        jsonMetricField(out, indentLevel + 2, "sleepBranch", tickManager.sleepBranch.nanos,
                tickManager.sleepBranch.calls, profileTicks, true);
        jsonMetricField(out, indentLevel + 2, "futureStop", tickManager.futureStop.nanos,
                tickManager.futureStop.calls, profileTicks, false);
        indent(out, indentLevel + 1).append("},\n");

        indent(out, indentLevel + 1).append("\"controls\": {\n");
        jsonMetricField(out, indentLevel + 2, "sleep", tickManager.sleep.nanos,
                tickManager.sleep.calls, profileTicks, true);
        jsonMetricField(out, indentLevel + 2, "wake", tickManager.wake.nanos,
                tickManager.wake.calls, profileTicks, true);
        jsonMetricField(out, indentLevel + 2, "alert", tickManager.alert.nanos,
                tickManager.alert.calls, profileTicks, false);
        indent(out, indentLevel + 1).append("},\n");

        indent(out, indentLevel + 1).append("\"modulation\": {\n");
        Modulation[] modulations = Modulation.values();
        for (int i = 0; i < modulations.length; i++) {
            Modulation modulation = modulations[i];
            long count = tickManager.modulations.getOrDefault(modulation, 0L);
            indent(out, indentLevel + 2).append(jsonQuote(modulation.label)).append(": {")
                    .append("\"count\": ").append(count).append(", ")
                    .append("\"perTick\": ").append(decimal(count / (double) profileTicks)).append('}');
            if (i + 1 < modulations.length) {
                out.append(',');
            }
            out.append('\n');
        }
        indent(out, indentLevel + 1).append("}\n");
        indent(out, indentLevel).append('}');
    }

    private static void appendPhysicalSpikeJson(StringBuilder out, int level, PhysicalSpikeSnapshot spike) {
        indent(out, level).append("\"spike\": {\n");
        jsonNumberField(out, level + 1, "callsSeen", spike.callsSeen, true);
        jsonNumberField(out, level + 1, "samplesCaptured", spike.samplesCaptured, true);
        indent(out, level + 1).append("\"reservoirSampled\": ").append(spike.reservoirSampled).append(",\n");
        indent(out, level + 1).append("\"timelineTruncated\": ").append(spike.timelineTruncated).append(",\n");
        indent(out, level + 1).append("\"operationOverflow\": ").append(spike.operationOverflow).append(",\n");
        indent(out, level + 1).append("\"p50Us\": ").append(decimal(spike.p50Us)).append(",\n");
        indent(out, level + 1).append("\"p95Us\": ").append(decimal(spike.p95Us)).append(",\n");
        indent(out, level + 1).append("\"p99Us\": ").append(decimal(spike.p99Us)).append(",\n");
        indent(out, level + 1).append("\"maxUs\": ").append(decimal(spike.maxUs)).append(",\n");
        jsonNumberField(out, level + 1, "maxTick", spike.maxTick, true);

        indent(out, level + 1).append("\"topEvents\": [");
        for (int i = 0; i < spike.topEvents.size(); i++) {
            SpikeEventSnapshot event = spike.topEvents.get(i);
            if (i > 0) out.append(',');
            out.append("{\"tick\": ").append(event.tick)
                    .append(", \"us\": ").append(decimal(event.nanos / 1000.0)).append('}');
        }
        out.append("],\n");

        indent(out, level + 1).append("\"timeline\": [");
        for (int i = 0; i < spike.timeline.size(); i++) {
            SpikeBucketSnapshot bucket = spike.timeline.get(i);
            if (i > 0) out.append(',');
            out.append("{\"startTick\": ").append(bucket.startTick)
                    .append(", \"calls\": ").append(bucket.calls)
                    .append(", \"totalUs\": ").append(decimal(bucket.totalNanos / 1000.0))
                    .append(", \"maxUs\": ").append(decimal(bucket.maxNanos / 1000.0)).append('}');
        }
        out.append("],\n");

        appendDistributionModesJson(out, level + 1, spike.modes, spike.modeGapRatio, spike.modeMedianRatio, true);
        indent(out, level + 1).append("\"operations\": [");
        for (int i = 0; i < spike.operations.size(); i++) {
            OperationSpikeSnapshot operation = spike.operations.get(i);
            if (i > 0) out.append(',');
            out.append("{\"name\": ").append(jsonQuote(operation.name))
                    .append(", \"callsSeen\": ").append(operation.callsSeen)
                    .append(", \"totalUs\": ").append(decimal(operation.totalNanos / 1000.0))
                    .append(", \"avgUs\": ").append(decimal(operation.callsSeen > 0 ? operation.totalNanos / 1000.0 / operation.callsSeen : 0.0))
                    .append(", \"samplesCaptured\": ").append(operation.samplesCaptured)
                    .append(", \"reservoirSampled\": ").append(operation.reservoirSampled)
                    .append(", \"p50Us\": ").append(decimal(operation.p50Us))
                    .append(", \"p95Us\": ").append(decimal(operation.p95Us))
                    .append(", \"p99Us\": ").append(decimal(operation.p99Us))
                    .append(", \"maxUs\": ").append(decimal(operation.maxUs))
                    .append(", \"maxTick\": ").append(operation.maxTick)
                    .append(", \"modeGapRatio\": ").append(decimal(operation.modeGapRatio))
                    .append(", \"modeMedianRatio\": ").append(decimal(operation.modeMedianRatio))
                    .append(", \"modes\": ");
            appendDistributionModesArrayJson(out, operation.modes);
            out.append(", \"topEvents\": [");
            for (int e = 0; e < operation.topEvents.size(); e++) {
                SpikeEventSnapshot event = operation.topEvents.get(e);
                if (e > 0) out.append(',');
                out.append("{\"tick\": ").append(event.tick)
                        .append(", \"us\": ").append(decimal(event.nanos / 1000.0)).append('}');
            }
            out.append("]}");
        }
        out.append("]\n");
        indent(out, level).append('}');
    }

    private static void appendDistributionModesJson(StringBuilder out, int level,
                                                    List<DistributionModeSnapshot> modes,
                                                    double gapRatio, double medianRatio, boolean comma) {
        indent(out, level).append("\"modeGapRatio\": ").append(decimal(gapRatio)).append(",\n");
        indent(out, level).append("\"modeMedianRatio\": ").append(decimal(medianRatio)).append(",\n");
        indent(out, level).append("\"modes\": ");
        appendDistributionModesArrayJson(out, modes);
        if (comma) out.append(',');
        out.append('\n');
    }

    private static void appendDistributionModesArrayJson(StringBuilder out, List<DistributionModeSnapshot> modes) {
        out.append('[');
        for (int i = 0; i < modes.size(); i++) {
            DistributionModeSnapshot mode = modes.get(i);
            if (i > 0) out.append(',');
            out.append("{\"sampleCount\": ").append(mode.sampleCount)
                    .append(", \"sampleSharePct\": ").append(decimal(mode.sampleSharePct))
                    .append(", \"minUs\": ").append(decimal(mode.minUs))
                    .append(", \"p50Us\": ").append(decimal(mode.p50Us))
                    .append(", \"p95Us\": ").append(decimal(mode.p95Us))
                    .append(", \"maxUs\": ").append(decimal(mode.maxUs)).append('}');
        }
        out.append(']');
    }

    private static void jsonMetricField(StringBuilder out, int level, String name, long nanos, int calls,
                                        int profileTicks, boolean comma) {
        indent(out, level).append(jsonQuote(name)).append(": ");
        appendMetricJson(out, nanos, calls, profileTicks);
        if (comma) {
            out.append(',');
        }
        out.append('\n');
    }

    private static void appendMetricJson(StringBuilder out, long nanos, int calls, int profileTicks) {
        double usPerTick = profileTicks <= 0 ? 0.0 : nanos / 1000.0 / profileTicks;
        double callsPerTick = profileTicks <= 0 ? 0.0 : calls / (double) profileTicks;
        double usPerCall = calls <= 0 ? 0.0 : nanos / 1000.0 / calls;
        out.append('{')
                .append("\"totalNanos\": ").append(nanos).append(", ")
                .append("\"calls\": ").append(calls).append(", ")
                .append("\"usPerTick\": ").append(decimal(usPerTick)).append(", ")
                .append("\"callsPerTick\": ").append(decimal(callsPerTick)).append(", ")
                .append("\"usPerCall\": ").append(decimal(usPerCall))
                .append('}');
    }

    private static void appendPosition(StringBuilder out, BlockPos pos) {
        if (pos == null) {
            out.append("null");
            return;
        }
        out.append('{')
                .append("\"x\": ").append(pos.getX()).append(", ")
                .append("\"y\": ").append(pos.getY()).append(", ")
                .append("\"z\": ").append(pos.getZ())
                .append('}');
    }

    private static void jsonField(StringBuilder out, int level, String name, String value, boolean comma) {
        indent(out, level).append(jsonQuote(name)).append(": ").append(jsonQuote(value));
        if (comma) {
            out.append(',');
        }
        out.append('\n');
    }

    private static void jsonNumberField(StringBuilder out, int level, String name, long value, boolean comma) {
        indent(out, level).append(jsonQuote(name)).append(": ").append(value);
        if (comma) {
            out.append(',');
        }
        out.append('\n');
    }

    private static StringBuilder indent(StringBuilder out, int level) {
        for (int i = 0; i < level; i++) {
            out.append("  ");
        }
        return out;
    }

    private static String decimal(double value) {
        if (!Double.isFinite(value)) {
            return "0";
        }
        return String.format(Locale.ROOT, "%.6f", value);
    }

    private static String jsonQuote(String value) {
        if (value == null) {
            return "null";
        }
        StringBuilder out = new StringBuilder(value.length() + 8).append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '\\': out.append("\\\\"); break;
                case '"': out.append("\\\""); break;
                case '\n': out.append("\\n"); break;
                case '\r': out.append("\\r"); break;
                case '\t': out.append("\\t"); break;
                default:
                    if (c < 0x20) {
                        out.append(String.format(Locale.ROOT, "\\u%04x", (int) c));
                    } else {
                        out.append(c);
                    }
            }
        }
        return out.append('"').toString();
    }

    /**
     * Materializes private counters into Observable's block map once, after the
     * live timing session has stopped. This preserves v7's snapshot-safe model.
     */
    public static void publishVirtualTimings() {
        sessionActive = false;

        // The standalone AE2 report now owns the detailed breakdown. Observable's
        // normal upload receives only one inclusive marker for the requested Top-N
        // grids, preventing thousands of grids x dozens of synthetic markers from
        // bloating the base profile and browser payload.
        List<GridSnapshot> snapshots = reportGridSnapshots(snapshotsSorted(), requestedReportGridLimit());
        Profiler profiler = Observable.INSTANCE.getPROFILER();

        for (GridSnapshot snapshot : snapshots) {
            if (snapshot.gridCoreNanos <= 0L || snapshot.gridCoreCalls <= 0
                    || snapshot.anchorEntity == null || snapshot.virtualBase == null
                    || !(snapshot.anchorEntity.getLevel() instanceof ServerLevel)) {
                continue;
            }
            publishMetric(profiler, snapshot.anchorEntity.getLevel(), snapshot.virtualBase, 0,
                    snapshot.label + " / Grid Core (inclusive)",
                    snapshot.gridCoreNanos, snapshot.gridCoreCalls);
        }
    }

    private static int publishMetric(Profiler profiler, Level level, BlockPos base, int slot,
                                     String name, long nanos, int calls) {
        if (slot >= MAX_VIRTUAL_MARKERS || nanos <= 0L || calls <= 0) {
            return slot;
        }

        Profiler.TimingData data = profiler.processCompatVirtualBlock(level, base.above(slot), name);
        synchronized (data) {
            data.setTime(nanos);
            data.setTicks(calls);
            data.setName(name);
        }
        return slot + 1;
    }

    private static List<GridSnapshot> snapshotsSorted() {
        List<GridInfo> infos;
        synchronized (LOCK) {
            infos = new ArrayList<>(GRIDS.values());
        }

        List<GridSnapshot> snapshots = new ArrayList<>();
        for (GridInfo info : infos) {
            snapshots.add(snapshot(info));
        }
        snapshots.sort(new Comparator<GridSnapshot>() {
            @Override
            public int compare(GridSnapshot left, GridSnapshot right) {
                return Long.compare(right.scoreNanos, left.scoreNanos);
            }
        });
        return snapshots;
    }

    private static void appendMetric(StringBuilder builder, String name, long nanos, int profileTicks) {
        if (nanos <= 0L) {
            return;
        }
        double usPerTick = nanos / (double) profileTicks / 1000.0;
        builder.append(" | ").append(name).append(' ')
                .append(String.format(Locale.ROOT, "%.2f", usPerTick))
                .append(" us/t");
    }

    private static void appendMetricWithCalls(StringBuilder builder, String name, MetricSnapshot metric, int profileTicks) {
        if (metric == null || metric.nanos <= 0L || metric.calls <= 0) {
            return;
        }
        double usPerTick = metric.nanos / (double) profileTicks / 1000.0;
        double callsPerTick = metric.calls / (double) profileTicks;
        builder.append(" | ").append(name).append(' ')
                .append(String.format(Locale.ROOT, "%.3f", usPerTick))
                .append(" us/t @ ")
                .append(String.format(Locale.ROOT, "%.2f", callsPerTick))
                .append("x/t");
    }

    private static void appendCountPerTick(StringBuilder builder, String name, long count, int profileTicks) {
        if (count <= 0L) {
            return;
        }
        builder.append(" | ").append(name).append(' ')
                .append(String.format(Locale.ROOT, "%.2f", count / (double) profileTicks))
                .append("x/t");
    }

    private static void appendPhaseMetric(StringBuilder builder, String name, MetricSnapshot metric, int profileTicks) {
        double usPerTick = metric.nanos / (double) profileTicks / 1000.0;
        double callsPerTick = metric.calls / (double) profileTicks;
        builder.append(" | ").append(name).append(' ')
                .append(String.format(Locale.ROOT, "%.2f", usPerTick))
                .append(" us/t @ ")
                .append(String.format(Locale.ROOT, "%.1f", callsPerTick))
                .append("x/t");
    }

    private static void appendKnownService(StringBuilder builder, List<ServiceSnapshot> services,
                                           KnownService known, String label, int profileTicks) {
        long nanos = 0L;
        for (ServiceSnapshot service : services) {
            if (service.known == known) {
                nanos += service.total.nanos;
            }
        }
        appendMetric(builder, label, nanos, profileTicks);
    }

    private static void recordLocked(Metric metric, long elapsed) {
        recordLocked(metric, elapsed, 1);
    }

    private static void recordLocked(Metric metric, long elapsed, int weight) {
        if (elapsed < 0L || weight <= 0) {
            return;
        }
        metric.nanos += elapsed * (long) weight;
        metric.calls += weight;
        metric.samples += 1;
    }

    private static GridInfo getOrCreateGrid(Object grid) {
        synchronized (LOCK) {
            GridInfo info = GRIDS.get(grid);
            if (info == null) {
                long serial = readGridSerial(grid);
                int fallback = NEXT_FALLBACK_ID.getAndIncrement();
                String label = serial >= 0L ? "AE2 Grid #" + serial : "AE2 Grid P" + fallback;
                info = new GridInfo(grid, label);
                GRIDS.put(grid, info);
                discoveredGridCount = GRIDS.size();
            }
            return info;
        }
    }

    private static ServiceInfo getOrCreateService(GridInfo info, Object service) {
        ServiceInfo serviceInfo;
        synchronized (info) {
            serviceInfo = info.services.get(service);
            if (serviceInfo == null) {
                Class<?> type = service.getClass();
                serviceInfo = new ServiceInfo(type.getName(), friendlyServiceName(type), knownService(type));
                info.services.put(service, serviceInfo);
            }
        }
        if (serviceInfo.known == KnownService.TICK_MANAGER) {
            synchronized (LOCK) {
                TICK_MANAGER_GRIDS.put(service, info);
            }
        }
        return serviceInfo;
    }

    private static void ensureAnchor(GridInfo info, BlockEntity fallback) {
        synchronized (info) {
            ensureAnchorLocked(info, fallback);
            ensureVirtualBaseLocked(info);
        }
    }

    private static void ensureAnchorLocked(GridInfo info, BlockEntity fallback) {
        if (info.anchor != null && !info.anchor.isRemoved() && info.anchor.getLevel() instanceof ServerLevel) {
            return;
        }

        BlockEntity discovered = discoverAnchor(info.grid);
        if (discovered == null) {
            discovered = fallback;
        }

        if (discovered != null && !discovered.isRemoved() && discovered.getLevel() instanceof ServerLevel) {
            info.anchor = discovered;
            info.virtualBase = null;
        }
    }

    private static BlockEntity discoverAnchor(Object grid) {
        BlockEntity pivot = CompatTiming.resolveBlockEntityForCompat(invokeNoArg(grid, "getPivot"));
        if (pivot != null) {
            return pivot;
        }

        Object nodes = invokeNoArg(grid, "getNodes");
        if (nodes instanceof Iterable<?>) {
            int inspected = 0;
            for (Object node : (Iterable<?>) nodes) {
                BlockEntity blockEntity = CompatTiming.resolveBlockEntityForCompat(node);
                if (blockEntity != null) {
                    return blockEntity;
                }
                if (++inspected >= 32) {
                    break;
                }
            }
        }
        return null;
    }

    private static void ensureVirtualBaseLocked(GridInfo info) {
        if (info.virtualBase != null || info.anchor == null || !(info.anchor.getLevel() instanceof ServerLevel)) {
            return;
        }

        ServerLevel level = (ServerLevel) info.anchor.getLevel();
        BlockPos anchor = info.anchor.getBlockPos();
        int height = MAX_VIRTUAL_MARKERS;

        for (int[] offset : VIRTUAL_OFFSETS) {
            BlockPos above = anchor.offset(offset[0], 2, offset[1]);
            if (above.getY() + height < level.getMaxBuildHeight() && isFreeColumn(level, above, height)) {
                reserveColumn(level, above, height);
                info.virtualBase = above;
                return;
            }
        }

        for (int[] offset : VIRTUAL_OFFSETS) {
            BlockPos below = anchor.offset(offset[0], -(height + 2), offset[1]);
            if (isFreeColumn(level, below, height)) {
                reserveColumn(level, below, height);
                info.virtualBase = below;
                return;
            }
        }

        for (int radius = 8; radius <= 48; radius += 2) {
            BlockPos fallback = anchor.offset(radius, 2, 0);
            if (!isReservedColumn(level, fallback, height)) {
                reserveColumn(level, fallback, height);
                info.virtualBase = fallback;
                return;
            }
        }
    }

    private static boolean isFreeColumn(ServerLevel level, BlockPos base, int height) {
        if (isReservedColumn(level, base, height)) {
            return false;
        }
        for (int i = 0; i < height; i++) {
            if (!level.isEmptyBlock(base.above(i))) {
                return false;
            }
        }
        return true;
    }

    private static boolean isReservedColumn(ServerLevel level, BlockPos base, int height) {
        synchronized (LOCK) {
            for (int i = 0; i < height; i++) {
                if (RESERVED_VIRTUAL_POSITIONS.contains(virtualKey(level, base.above(i)))) {
                    return true;
                }
            }
            return false;
        }
    }

    private static void reserveColumn(ServerLevel level, BlockPos base, int height) {
        synchronized (LOCK) {
            for (int i = 0; i < height; i++) {
                RESERVED_VIRTUAL_POSITIONS.add(virtualKey(level, base.above(i)));
            }
        }
    }

    private static String virtualKey(ServerLevel level, BlockPos pos) {
        return level.dimension().toString() + ':' + pos.asLong();
    }

    private static GridInfo findGridForTickManager(Object tickManager) {
        synchronized (LOCK) {
            GridInfo known = TICK_MANAGER_GRIDS.get(tickManager);
            if (known != null) {
                return known;
            }
        }

        Object grid = invokeNoArg(tickManager, "getGrid");
        if (!isGridObject(grid)) {
            grid = readField(tickManager, "grid");
        }
        if (!isGridObject(grid)) {
            grid = readField(tickManager, "myGrid");
        }
        if (!isGridObject(grid)) {
            return null;
        }

        GridInfo info = getOrCreateGrid(grid);
        synchronized (LOCK) {
            TICK_MANAGER_GRIDS.put(tickManager, info);
        }
        return info;
    }

    private static TickManagerSectionToken removeMatchingSectionToken(Object tickManager, TickSection section) {
        ArrayDeque<TickManagerSectionToken> stack = TICK_MANAGER_SECTION_STACK.get();
        if (!stack.isEmpty() && stack.peek().tickManager == tickManager && stack.peek().section == section) {
            return stack.pop();
        }
        Iterator<TickManagerSectionToken> iterator = stack.iterator();
        while (iterator.hasNext()) {
            TickManagerSectionToken candidate = iterator.next();
            if (candidate.tickManager == tickManager && candidate.section == section) {
                iterator.remove();
                return candidate;
            }
        }
        return null;
    }

    private static TickManagerControlToken removeMatchingControlToken(Object tickManager, TickControl control) {
        ArrayDeque<TickManagerControlToken> stack = TICK_MANAGER_CONTROL_STACK.get();
        if (!stack.isEmpty() && stack.peek().tickManager == tickManager && stack.peek().control == control) {
            return stack.pop();
        }
        Iterator<TickManagerControlToken> iterator = stack.iterator();
        while (iterator.hasNext()) {
            TickManagerControlToken candidate = iterator.next();
            if (candidate.tickManager == tickManager && candidate.control == control) {
                iterator.remove();
                return candidate;
            }
        }
        return null;
    }

    private static Metric controlMetric(TickManagerInfo tickManager, TickControl control) {
        switch (control) {
            case SLEEP:
                return tickManager.sleep;
            case WAKE:
                return tickManager.wake;
            case ALERT:
                return tickManager.alert;
            default:
                throw new IllegalArgumentException("Unknown tick control " + control);
        }
    }

    private static void recordModulationLocked(TickManagerInfo tickManager, Object modulation) {
        Modulation resolved = Modulation.fromValue(modulation);
        if (resolved != null) {
            Long count = tickManager.modulations.get(resolved);
            tickManager.modulations.put(resolved, count == null ? 1L : count + 1L);
        }
    }

    private static Object findGridFromNode(Object node) {
        Object grid = invokeNoArg(node, "getGrid");
        return isGridObject(grid) ? grid : null;
    }

    private static boolean isGridObject(Object value) {
        if (value == null) {
            return false;
        }
        Class<?> type = value.getClass();
        if ("appeng.me.Grid".equals(type.getName())) {
            return true;
        }
        return implementsInterface(type, "appeng.api.networking.IGrid");
    }

    private static boolean implementsInterface(Class<?> type, String interfaceName) {
        for (Class<?> current = type; current != null && current != Object.class; current = current.getSuperclass()) {
            for (Class<?> iface : current.getInterfaces()) {
                if (interfaceName.equals(iface.getName()) || implementsInterface(iface, interfaceName)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static KnownService knownService(Class<?> type) {
        String name = type.getName();
        if (name.endsWith(".TickManagerService")) {
            return KnownService.TICK_MANAGER;
        }
        if (name.endsWith(".StorageService")) {
            return KnownService.STORAGE;
        }
        if (name.endsWith(".CraftingService")) {
            return KnownService.CRAFTING;
        }
        if (name.endsWith(".EnergyService")) {
            return KnownService.ENERGY;
        }
        if (name.endsWith(".PathingService")) {
            return KnownService.PATHING;
        }
        if (name.endsWith(".SpatialService")) {
            return KnownService.SPATIAL;
        }
        if (name.endsWith(".P2PService")) {
            return KnownService.P2P;
        }
        if (name.endsWith(".SecurityService")) {
            return KnownService.SECURITY;
        }
        return KnownService.OTHER;
    }

    private static String friendlyServiceName(Class<?> type) {
        String simple = type.getSimpleName();
        if (simple == null || simple.isEmpty()) {
            simple = type.getName();
        }

        KnownService known = knownService(type);
        switch (known) {
            case TICK_MANAGER:
                return "Tick Manager";
            case STORAGE:
                return "Storage";
            case CRAFTING:
                return "Crafting";
            case ENERGY:
                return "Energy";
            case PATHING:
                return "Pathing";
            case SPATIAL:
                return "Spatial";
            case P2P:
                return "P2P";
            case SECURITY:
                return "Security";
            default:
                break;
        }

        if (simple.endsWith("Service") && simple.length() > "Service".length()) {
            simple = simple.substring(0, simple.length() - "Service".length());
        }

        String className = type.getName();
        if (!className.startsWith("appeng.")) {
            String packageHint = packageHint(className);
            if (!packageHint.isEmpty()) {
                return simple + " [" + packageHint + "]";
            }
        }
        return simple;
    }

    private static String packageHint(String className) {
        int first = className.indexOf('.');
        if (first < 0) {
            return "";
        }
        int second = className.indexOf('.', first + 1);
        if (second < 0) {
            return className.substring(0, first);
        }
        return className.substring(0, second);
    }

    private static long readGridSerial(Object grid) {
        Object result = invokeNoArg(grid, "getSerialNumber");
        if (result instanceof Number) {
            return ((Number) result).longValue();
        }

        Object fieldValue = readField(grid, "serialNumber");
        if (fieldValue instanceof Number) {
            return ((Number) fieldValue).longValue();
        }
        return -1L;
    }

    private static Object invokeNoArg(Object source, String name) {
        if (source == null) {
            return null;
        }

        for (Class<?> type = source.getClass(); type != null && type != Object.class; type = type.getSuperclass()) {
            try {
                Method method = type.getDeclaredMethod(name);
                if (method.getParameterCount() != 0) {
                    return null;
                }
                method.setAccessible(true);
                return method.invoke(source);
            } catch (NoSuchMethodException ignored) {
                // Keep walking the hierarchy.
            } catch (ReflectiveOperationException | RuntimeException ignored) {
                return null;
            }
        }

        try {
            Method method = source.getClass().getMethod(name);
            if (method.getParameterCount() == 0) {
                return method.invoke(source);
            }
        } catch (ReflectiveOperationException | RuntimeException ignored) {
            // Optional compat must fail soft.
        }
        return null;
    }

    private static Object readField(Object source, String name) {
        if (source == null) {
            return null;
        }
        for (Class<?> type = source.getClass(); type != null && type != Object.class; type = type.getSuperclass()) {
            try {
                Field field = type.getDeclaredField(name);
                if (Modifier.isStatic(field.getModifiers())) {
                    return null;
                }
                field.setAccessible(true);
                return field.get(source);
            } catch (NoSuchFieldException ignored) {
                // Keep walking the hierarchy.
            } catch (IllegalAccessException | RuntimeException ignored) {
                return null;
            }
        }
        return null;
    }

    private static boolean isServerProfilerThread() {
        try {
            return Thread.currentThread() == Observable.INSTANCE.getPROFILER().getServerThread();
        } catch (Throwable ignored) {
            return false;
        }
    }

    private static GridSnapshot snapshot(GridInfo info) {
        synchronized (info) {
            long gridCoreNanos = info.gridCore.nanos;
            int gridCoreCalls = info.gridCore.calls;
            long deviceNanos = info.devices.nanos;
            int deviceCalls = info.devices.calls;

            EnumMap<Phase, MetricSnapshot> phases = new EnumMap<>(Phase.class);
            for (Map.Entry<Phase, Metric> entry : info.gridPhases.entrySet()) {
                phases.put(entry.getKey(), new MetricSnapshot(entry.getValue().nanos, entry.getValue().calls));
            }

            List<ServiceSnapshot> services = new ArrayList<>();
            long serviceTotalNanos = 0L;
            int serviceTotalCalls = 0;
            long tickManagerServiceNanos = 0L;
            int tickManagerServiceCalls = 0;
            for (ServiceInfo service : info.services.values()) {
                MetricSnapshot total = new MetricSnapshot(service.total.nanos, service.total.calls);
                EnumMap<Phase, MetricSnapshot> servicePhases = new EnumMap<>(Phase.class);
                for (Map.Entry<Phase, Metric> entry : service.phases.entrySet()) {
                    servicePhases.put(entry.getKey(), new MetricSnapshot(entry.getValue().nanos, entry.getValue().calls));
                }
                services.add(new ServiceSnapshot(service.className, service.displayName, service.known, total, servicePhases));
                serviceTotalNanos += total.nanos;
                serviceTotalCalls += total.calls;
                if (service.known == KnownService.TICK_MANAGER) {
                    tickManagerServiceNanos += total.nanos;
                    tickManagerServiceCalls += total.calls;
                }
            }

            services.sort(new Comparator<ServiceSnapshot>() {
                @Override
                public int compare(ServiceSnapshot left, ServiceSnapshot right) {
                    return Long.compare(right.total.nanos, left.total.nanos);
                }
            });

            long gridOverheadNanos = Math.max(0L, gridCoreNanos - serviceTotalNanos);

            Level anchorLevel = info.anchor == null ? null : info.anchor.getLevel();
            TickManagerSnapshot tickManager = snapshotTickManager(
                    info.tickManager, tickManagerServiceNanos, tickManagerServiceCalls, deviceNanos, deviceCalls, anchorLevel);

            long score = Math.max(gridCoreNanos, serviceTotalNanos);
            BlockPos anchor = info.anchor == null ? null : info.anchor.getBlockPos();

            return new GridSnapshot(
                    info.label,
                    anchor,
                    info.anchor,
                    info.virtualBase,
                    gridCoreNanos,
                    gridCoreCalls,
                    phases,
                    serviceTotalNanos,
                    serviceTotalCalls,
                    gridOverheadNanos,
                    deviceNanos,
                    deviceCalls,
                    services,
                    tickManager,
                    score,
                    runtimeGridSelected(info.grid));
        }
    }

    private static TickManagerSnapshot snapshotTickManager(
            TickManagerInfo info, long serviceNanos, int serviceCalls, long deviceNanos, int deviceCalls, Level anchorLevel) {
        MetricSnapshot levelQueue = new MetricSnapshot(info.levelQueue.nanos, info.levelQueue.calls);
        MetricSnapshot queue = new MetricSnapshot(info.queue.nanos, info.queue.calls);
        MetricSnapshot sleep = new MetricSnapshot(info.sleep.nanos, info.sleep.calls);
        MetricSnapshot wake = new MetricSnapshot(info.wake.nanos, info.wake.calls);
        MetricSnapshot alert = new MetricSnapshot(info.alert.nanos, info.alert.calls);
        MetricSnapshot headDueCheck = new MetricSnapshot(info.headDueCheck.nanos, info.headDueCheck.calls);
        MetricSnapshot dequeuePrep = new MetricSnapshot(info.dequeuePrep.nanos, info.dequeuePrep.calls);
        MetricSnapshot rateUpdate = new MetricSnapshot(info.rateUpdate.nanos, info.rateUpdate.calls);
        MetricSnapshot sleepBranch = new MetricSnapshot(info.sleepBranch.nanos, info.sleepBranch.calls);
        MetricSnapshot awakeCheck = new MetricSnapshot(info.awakeCheck.nanos, info.awakeCheck.calls);
        MetricSnapshot reinsert = new MetricSnapshot(info.reinsert.nanos, info.reinsert.calls);
        MetricSnapshot futureStop = new MetricSnapshot(info.futureStop.nanos, info.futureStop.calls);

        long queueBookkeeping = Math.max(0L, queue.nanos - deviceNanos);
        long detailedSchedulerNanos = headDueCheck.nanos + dequeuePrep.nanos + rateUpdate.nanos
                + sleepBranch.nanos + awakeCheck.nanos + reinsert.nanos + futureStop.nanos;
        long queueResidual = Math.max(0L, queueBookkeeping - detailedSchedulerNanos);
        long levelDispatch = Math.max(0L, levelQueue.nanos - queue.nanos);
        long outerOverhead = Math.max(0L, serviceNanos - levelQueue.nanos);

        EnumMap<Modulation, Long> modulations = new EnumMap<>(Modulation.class);
        modulations.putAll(info.modulations);

        List<LevelDispatchSnapshot> dispatchLevels = new ArrayList<>();
        for (int levelId = 0; levelId < info.levelDispatches.size(); levelId++) {
            LevelDispatchInfo dispatch = info.levelDispatches.get(levelId);
            if (dispatch == null) {
                continue;
            }
            Level level = levelId < DISPATCH_LEVELS.size() ? DISPATCH_LEVELS.get(levelId) : null;
            MetricSnapshot start = new MetricSnapshot(dispatch.levelStart.nanos, dispatch.levelStart.calls);
            MetricSnapshot end = new MetricSnapshot(dispatch.levelEnd.nanos, dispatch.levelEnd.calls);
            MetricSnapshot total = new MetricSnapshot(start.nanos + end.nanos, start.calls + end.calls);
            String dimension = "unknown";
            try {
                if (level != null) {
                    dimension = level.dimension().location().toString();
                }
            } catch (Throwable ignored) {
                // Keep report generation fail-soft for custom Level implementations.
            }
            dispatchLevels.add(new LevelDispatchSnapshot(
                    dimension, anchorLevel != null && anchorLevel == level, total, start, end));
        }
        dispatchLevels.sort(new Comparator<LevelDispatchSnapshot>() {
            @Override
            public int compare(LevelDispatchSnapshot left, LevelDispatchSnapshot right) {
                int byCalls = Integer.compare(right.total.calls, left.total.calls);
                return byCalls != 0 ? byCalls : Long.compare(right.total.nanos, left.total.nanos);
            }
        });

        return new TickManagerSnapshot(
                serviceNanos, serviceCalls, levelQueue, queue,
                queueBookkeeping, queueResidual, levelDispatch, outerOverhead,
                headDueCheck, dequeuePrep, rateUpdate, sleepBranch, awakeCheck, reinsert, futureStop,
                sleep, wake, alert, modulations, dispatchLevels, deviceNanos, deviceCalls);
    }

    private enum Phase {
        SERVER_START(PHASE_SERVER_START, "SStart"),
        LEVEL_START(PHASE_LEVEL_START, "LStart"),
        LEVEL_END(PHASE_LEVEL_END, "LEnd"),
        SERVER_END(PHASE_SERVER_END, "SEnd");

        private final int id;
        private final String shortName;

        Phase(int id, String shortName) {
            this.id = id;
            this.shortName = shortName;
        }

        private static Phase fromId(int id) {
            for (Phase phase : values()) {
                if (phase.id == id) {
                    return phase;
                }
            }
            return null;
        }
    }

    private enum TickSection {
        LEVEL_QUEUE(TICK_SECTION_LEVEL_QUEUE),
        QUEUE(TICK_SECTION_QUEUE);

        private final int id;

        TickSection(int id) {
            this.id = id;
        }

        private static TickSection fromId(int id) {
            for (TickSection section : values()) {
                if (section.id == id) {
                    return section;
                }
            }
            return null;
        }
    }

    private enum TickQueuePhase {
        NONE,
        HEAD_CHECK,
        HEAD_DUE,
        DEQUEUE_PREP,
        RATE_UPDATE,
        SLEEP_BRANCH,
        AWAKE_CHECK,
        REINSERT,
        FUTURE_STOP
    }

    private enum TickControl {
        SLEEP(TICK_CONTROL_SLEEP),
        WAKE(TICK_CONTROL_WAKE),
        ALERT(TICK_CONTROL_ALERT);

        private final int id;

        TickControl(int id) {
            this.id = id;
        }

        private static TickControl fromId(int id) {
            for (TickControl control : values()) {
                if (control.id == id) {
                    return control;
                }
            }
            return null;
        }
    }

    private enum Modulation {
        SLEEP("Sleep"),
        IDLE("Idle"),
        SLOWER("Slower"),
        SAME("Same"),
        FASTER("Faster"),
        URGENT("Urgent");

        private final String label;

        Modulation(String label) {
            this.label = label;
        }

        private static Modulation fromValue(Object value) {
            if (value == null) {
                return null;
            }
            String name;
            if (value instanceof Enum<?>) {
                name = ((Enum<?>) value).name();
            } else {
                name = String.valueOf(value);
            }
            if (name == null) {
                return null;
            }
            name = name.trim().toUpperCase(Locale.ROOT);
            for (Modulation modulation : values()) {
                if (modulation.name().equals(name)) {
                    return modulation;
                }
            }
            return null;
        }
    }

    private enum KnownService {
        TICK_MANAGER,
        STORAGE,
        CRAFTING,
        ENERGY,
        PATHING,
        SPATIAL,
        P2P,
        SECURITY,
        OTHER
    }

    private static final class GridInfo {
        private final Object grid;
        private final String label;
        private final Metric gridCore = new Metric();
        private final EnumMap<Phase, Metric> gridPhases = new EnumMap<>(Phase.class);
        private final Metric devices = new Metric();
        private final TickManagerInfo tickManager = new TickManagerInfo();
        private final IdentityHashMap<Object, ServiceInfo> services = new IdentityHashMap<>();
        private BlockEntity anchor;
        private BlockPos virtualBase;

        private GridInfo(Object grid, String label) {
            this.grid = grid;
            this.label = label;
            for (Phase phase : Phase.values()) {
                gridPhases.put(phase, new Metric());
            }
        }
    }

    private static final class TickManagerInfo {
        private final Metric levelQueue = new Metric();
        private final Metric queue = new Metric();
        private final Metric headDueCheck = new Metric();
        private final Metric dequeuePrep = new Metric();
        private final Metric rateUpdate = new Metric();
        private final Metric sleepBranch = new Metric();
        private final Metric awakeCheck = new Metric();
        private final Metric reinsert = new Metric();
        private final Metric futureStop = new Metric();
        private final Metric sleep = new Metric();
        private final Metric wake = new Metric();
        private final Metric alert = new Metric();
        private final EnumMap<Modulation, Long> modulations = new EnumMap<>(Modulation.class);
        private final ArrayList<LevelDispatchInfo> levelDispatches = new ArrayList<>();

        private LevelDispatchInfo dispatchFor(int levelId) {
            while (levelDispatches.size() <= levelId) {
                levelDispatches.add(null);
            }
            LevelDispatchInfo dispatch = levelDispatches.get(levelId);
            if (dispatch == null) {
                dispatch = new LevelDispatchInfo();
                levelDispatches.set(levelId, dispatch);
            }
            return dispatch;
        }
    }

    private static final class DispatchLevelCache {
        private Level level;
        private int id = -1;
    }

    private static final class LevelDispatchInfo {
        private final Metric levelStart = new Metric();
        private final Metric levelEnd = new Metric();
    }

    private static final class ServiceInfo {
        private final String className;
        private final String displayName;
        private final KnownService known;
        private final Metric total = new Metric();
        private final EnumMap<Phase, Metric> phases = new EnumMap<>(Phase.class);

        private ServiceInfo(String className, String displayName, KnownService known) {
            this.className = className;
            this.displayName = displayName;
            this.known = known;
            for (Phase phase : Phase.values()) {
                phases.put(phase, new Metric());
            }
        }
    }

    private static final class Metric {
        private long nanos;
        private int calls;
        private int samples;
    }

    private static final class MetricSnapshot {
        private final long nanos;
        private final int calls;

        private MetricSnapshot(long nanos, int calls) {
            this.nanos = nanos;
            this.calls = calls;
        }
    }

    private static final class PhysicalDeviceRef {
        private BlockEntity host;
        private final String dimension;
        private final BlockPos position;
        private final SpikeStats spikes = new SpikeStats();
        private String preferredName;
        private String gridLabel;

        private PhysicalDeviceRef(BlockEntity host, String dimension, BlockPos position, String preferredName) {
            this.host = host;
            this.dimension = dimension;
            this.position = position;
            this.preferredName = preferredName;
        }
    }

    private static final class SpikeStats {
        private final long[] reservoir = new long[SPIKE_RESERVOIR_SIZE];
        private final long[] topNanos = new long[SPIKE_TOP_EVENTS];
        private final int[] topTicks = new int[SPIKE_TOP_EVENTS];
        private final long[] bucketTotalNanos = new long[SPIKE_MAX_BUCKETS];
        private final long[] bucketMaxNanos = new long[SPIKE_MAX_BUCKETS];
        private final int[] bucketCalls = new int[SPIKE_MAX_BUCKETS];
        private Map<String, OperationSpikeStats> operations;
        private long callsSeen;
        private int reservoirCount;
        private long randomState = 0x9e3779b97f4a7c15L;
        private boolean timelineTruncated;
        private boolean operationOverflow;

        private void record(long elapsedNanos, int tickOffset, String operation) {
            callsSeen++;
            if (reservoirCount < reservoir.length) {
                reservoir[reservoirCount++] = elapsedNanos;
            } else {
                reservoirSample(reservoir, callsSeen, elapsedNanos);
            }

            insertTopEvent(topNanos, topTicks, elapsedNanos, tickOffset);

            if (tickOffset >= 0) {
                int bucket = tickOffset / SPIKE_BUCKET_TICKS;
                if (bucket >= 0 && bucket < SPIKE_MAX_BUCKETS) {
                    bucketCalls[bucket]++;
                    bucketTotalNanos[bucket] += elapsedNanos;
                    bucketMaxNanos[bucket] = Math.max(bucketMaxNanos[bucket], elapsedNanos);
                } else if (bucket >= SPIKE_MAX_BUCKETS) {
                    timelineTruncated = true;
                }
            }

            recordOperation(operation, elapsedNanos, tickOffset);
        }

        private void reservoirSample(long[] target, long seen, long elapsedNanos) {
            // Deterministic xorshift reservoir sampling; no Random allocation on the hot path.
            long x = randomState;
            x ^= x << 13;
            x ^= x >>> 7;
            x ^= x << 17;
            randomState = x;
            long positive = x & Long.MAX_VALUE;
            long slot = positive % seen;
            if (slot < target.length) {
                target[(int) slot] = elapsedNanos;
            }
        }

        private void recordOperation(String operation, long elapsedNanos, int tickOffset) {
            String key = operation == null || operation.isBlank() ? "unknown" : operation;
            if (operations == null) {
                operations = new HashMap<>();
            }
            OperationSpikeStats stats = operations.get(key);
            if (stats == null) {
                if (operations.size() >= SPIKE_MAX_OPERATION_KINDS - 1) {
                    key = "other";
                    operationOverflow = true;
                    stats = operations.get(key);
                }
                if (stats == null) {
                    stats = new OperationSpikeStats();
                    operations.put(key, stats);
                }
            }
            stats.record(elapsedNanos, tickOffset);
        }

        private static void insertTopEvent(long[] nanos, int[] ticks, long elapsedNanos, int tickOffset) {
            if (elapsedNanos <= nanos[nanos.length - 1]) {
                return;
            }
            int at = nanos.length - 1;
            while (at > 0 && elapsedNanos > nanos[at - 1]) {
                nanos[at] = nanos[at - 1];
                ticks[at] = ticks[at - 1];
                at--;
            }
            nanos[at] = elapsedNanos;
            ticks[at] = tickOffset;
        }

        private PhysicalSpikeSnapshot snapshot() {
            long[] sorted = Arrays.copyOf(reservoir, reservoirCount);
            Arrays.sort(sorted);
            List<SpikeEventSnapshot> events = snapshotsOfTop(topNanos, topTicks);
            List<SpikeBucketSnapshot> timeline = new ArrayList<>();
            for (int i = 0; i < bucketCalls.length; i++) {
                if (bucketCalls[i] <= 0) continue;
                timeline.add(new SpikeBucketSnapshot(i * SPIKE_BUCKET_TICKS, bucketCalls[i],
                        bucketTotalNanos[i], bucketMaxNanos[i]));
            }
            List<OperationSpikeSnapshot> operationSnapshots = new ArrayList<>();
            if (operations != null) {
                for (Map.Entry<String, OperationSpikeStats> entry : operations.entrySet()) {
                    operationSnapshots.add(entry.getValue().snapshot(entry.getKey()));
                }
                operationSnapshots.sort(new Comparator<OperationSpikeSnapshot>() {
                    @Override
                    public int compare(OperationSpikeSnapshot left, OperationSpikeSnapshot right) {
                        int byTotal = Long.compare(right.totalNanos, left.totalNanos);
                        if (byTotal != 0) return byTotal;
                        return left.name.compareTo(right.name);
                    }
                });
            }
            double p50 = percentileUs(sorted, 0.50);
            double p95 = percentileUs(sorted, 0.95);
            double p99 = percentileUs(sorted, 0.99);
            double max = topNanos[0] / 1000.0;
            int maxTick = topNanos[0] > 0L ? topTicks[0] : -1;
            DistributionModesResult modes = distributionModes(sorted);
            return new PhysicalSpikeSnapshot(callsSeen, reservoirCount, callsSeen > reservoirCount,
                    timelineTruncated, operationOverflow, p50, p95, p99, max, maxTick,
                    modes.gapRatio, modes.medianRatio, modes.modes, events, timeline, operationSnapshots);
        }

        private static List<SpikeEventSnapshot> snapshotsOfTop(long[] nanos, int[] ticks) {
            List<SpikeEventSnapshot> events = new ArrayList<>();
            for (int i = 0; i < nanos.length && nanos[i] > 0L; i++) {
                events.add(new SpikeEventSnapshot(ticks[i], nanos[i]));
            }
            return events;
        }

        private static double percentileUs(long[] sorted, double percentile) {
            if (sorted.length == 0) return 0.0;
            int index = (int) Math.ceil(percentile * sorted.length) - 1;
            index = Math.max(0, Math.min(sorted.length - 1, index));
            return sorted[index] / 1000.0;
        }
        private static DistributionModesResult distributionModes(long[] sorted) {
            if (sorted == null || sorted.length < SPIKE_MODE_MIN_ABSOLUTE_SAMPLES * 2) {
                return DistributionModesResult.EMPTY;
            }
            int minGroup = Math.max(SPIKE_MODE_MIN_ABSOLUTE_SAMPLES,
                    (int) Math.ceil(sorted.length * SPIKE_MODE_MIN_SHARE));
            if (minGroup * 2 > sorted.length) {
                return DistributionModesResult.EMPTY;
            }

            int bestSplit = -1;
            double bestGapRatio = 0.0;
            for (int split = minGroup; split <= sorted.length - minGroup; split++) {
                long low = sorted[split - 1];
                long high = sorted[split];
                if (low <= 0L || high <= low) continue;
                double gapRatio = high / (double) low;
                if (gapRatio > bestGapRatio) {
                    bestGapRatio = gapRatio;
                    bestSplit = split;
                }
            }
            if (bestSplit < 0 || bestGapRatio < SPIKE_MODE_MIN_GAP_RATIO) {
                return DistributionModesResult.EMPTY;
            }

            long[] low = Arrays.copyOfRange(sorted, 0, bestSplit);
            long[] high = Arrays.copyOfRange(sorted, bestSplit, sorted.length);
            double lowMedian = percentileUs(low, 0.50);
            double highMedian = percentileUs(high, 0.50);
            double medianRatio = lowMedian > 0.0 ? highMedian / lowMedian : 0.0;
            if (medianRatio < SPIKE_MODE_MIN_MEDIAN_RATIO) {
                return DistributionModesResult.EMPTY;
            }

            List<DistributionModeSnapshot> modes = new ArrayList<>(2);
            modes.add(distributionMode(low, sorted.length));
            modes.add(distributionMode(high, sorted.length));
            return new DistributionModesResult(bestGapRatio, medianRatio, modes);
        }

        private static DistributionModeSnapshot distributionMode(long[] samples, int totalSamples) {
            return new DistributionModeSnapshot(samples.length,
                    totalSamples > 0 ? samples.length * 100.0 / totalSamples : 0.0,
                    samples.length == 0 ? 0.0 : samples[0] / 1000.0,
                    percentileUs(samples, 0.50),
                    percentileUs(samples, 0.95),
                    samples.length == 0 ? 0.0 : samples[samples.length - 1] / 1000.0);
        }
    }

    private static final class OperationSpikeStats {
        private final long[] reservoir = new long[SPIKE_OPERATION_RESERVOIR_SIZE];
        private final long[] topNanos = new long[SPIKE_OPERATION_TOP_EVENTS];
        private final int[] topTicks = new int[SPIKE_OPERATION_TOP_EVENTS];
        private long callsSeen;
        private long totalNanos;
        private int reservoirCount;
        private long randomState = 0xd1b54a32d192ed03L;

        private void record(long elapsedNanos, int tickOffset) {
            callsSeen++;
            totalNanos += elapsedNanos;
            if (reservoirCount < reservoir.length) {
                reservoir[reservoirCount++] = elapsedNanos;
            } else {
                long x = randomState;
                x ^= x << 13;
                x ^= x >>> 7;
                x ^= x << 17;
                randomState = x;
                long slot = (x & Long.MAX_VALUE) % callsSeen;
                if (slot < reservoir.length) {
                    reservoir[(int) slot] = elapsedNanos;
                }
            }
            SpikeStats.insertTopEvent(topNanos, topTicks, elapsedNanos, tickOffset);
        }

        private OperationSpikeSnapshot snapshot(String name) {
            long[] sorted = Arrays.copyOf(reservoir, reservoirCount);
            Arrays.sort(sorted);
            DistributionModesResult modes = SpikeStats.distributionModes(sorted);
            return new OperationSpikeSnapshot(name, callsSeen, totalNanos, reservoirCount,
                    callsSeen > reservoirCount,
                    SpikeStats.percentileUs(sorted, 0.50),
                    SpikeStats.percentileUs(sorted, 0.95),
                    SpikeStats.percentileUs(sorted, 0.99),
                    topNanos[0] / 1000.0,
                    topNanos[0] > 0L ? topTicks[0] : -1,
                    modes.gapRatio, modes.medianRatio, modes.modes,
                    SpikeStats.snapshotsOfTop(topNanos, topTicks));
        }
    }

    private static final class PhysicalSpikeSnapshot {
        private final long callsSeen;
        private final int samplesCaptured;
        private final boolean reservoirSampled;
        private final boolean timelineTruncated;
        private final boolean operationOverflow;
        private final double p50Us;
        private final double p95Us;
        private final double p99Us;
        private final double maxUs;
        private final int maxTick;
        private final double modeGapRatio;
        private final double modeMedianRatio;
        private final List<DistributionModeSnapshot> modes;
        private final List<SpikeEventSnapshot> topEvents;
        private final List<SpikeBucketSnapshot> timeline;
        private final List<OperationSpikeSnapshot> operations;

        private PhysicalSpikeSnapshot(long callsSeen, int samplesCaptured, boolean reservoirSampled,
                                      boolean timelineTruncated, boolean operationOverflow,
                                      double p50Us, double p95Us, double p99Us,
                                      double maxUs, int maxTick, double modeGapRatio, double modeMedianRatio,
                                      List<DistributionModeSnapshot> modes, List<SpikeEventSnapshot> topEvents,
                                      List<SpikeBucketSnapshot> timeline,
                                      List<OperationSpikeSnapshot> operations) {
            this.callsSeen = callsSeen;
            this.samplesCaptured = samplesCaptured;
            this.reservoirSampled = reservoirSampled;
            this.timelineTruncated = timelineTruncated;
            this.operationOverflow = operationOverflow;
            this.p50Us = p50Us;
            this.p95Us = p95Us;
            this.p99Us = p99Us;
            this.maxUs = maxUs;
            this.maxTick = maxTick;
            this.modeGapRatio = modeGapRatio;
            this.modeMedianRatio = modeMedianRatio;
            this.modes = modes;
            this.topEvents = topEvents;
            this.timeline = timeline;
            this.operations = operations;
        }
    }

    private static final class OperationSpikeSnapshot {
        private final String name;
        private final long callsSeen;
        private final long totalNanos;
        private final int samplesCaptured;
        private final boolean reservoirSampled;
        private final double p50Us;
        private final double p95Us;
        private final double p99Us;
        private final double maxUs;
        private final int maxTick;
        private final double modeGapRatio;
        private final double modeMedianRatio;
        private final List<DistributionModeSnapshot> modes;
        private final List<SpikeEventSnapshot> topEvents;

        private OperationSpikeSnapshot(String name, long callsSeen, long totalNanos, int samplesCaptured,
                                       boolean reservoirSampled, double p50Us, double p95Us, double p99Us,
                                       double maxUs, int maxTick, double modeGapRatio, double modeMedianRatio,
                                       List<DistributionModeSnapshot> modes, List<SpikeEventSnapshot> topEvents) {
            this.name = name;
            this.callsSeen = callsSeen;
            this.totalNanos = totalNanos;
            this.samplesCaptured = samplesCaptured;
            this.reservoirSampled = reservoirSampled;
            this.p50Us = p50Us;
            this.p95Us = p95Us;
            this.p99Us = p99Us;
            this.maxUs = maxUs;
            this.maxTick = maxTick;
            this.modeGapRatio = modeGapRatio;
            this.modeMedianRatio = modeMedianRatio;
            this.modes = modes;
            this.topEvents = topEvents;
        }
    }

    private static final class DistributionModesResult {
        private static final DistributionModesResult EMPTY = new DistributionModesResult(0.0, 0.0,
                Collections.emptyList());
        private final double gapRatio;
        private final double medianRatio;
        private final List<DistributionModeSnapshot> modes;

        private DistributionModesResult(double gapRatio, double medianRatio, List<DistributionModeSnapshot> modes) {
            this.gapRatio = gapRatio;
            this.medianRatio = medianRatio;
            this.modes = modes;
        }
    }

    private static final class DistributionModeSnapshot {
        private final int sampleCount;
        private final double sampleSharePct;
        private final double minUs;
        private final double p50Us;
        private final double p95Us;
        private final double maxUs;

        private DistributionModeSnapshot(int sampleCount, double sampleSharePct, double minUs,
                                         double p50Us, double p95Us, double maxUs) {
            this.sampleCount = sampleCount;
            this.sampleSharePct = sampleSharePct;
            this.minUs = minUs;
            this.p50Us = p50Us;
            this.p95Us = p95Us;
            this.maxUs = maxUs;
        }
    }

    private static final class ReportFileGroup {
        private final String baseName;
        private final List<Path> files = new ArrayList<>(2);
        private long newestModifiedMillis;

        private ReportFileGroup(String baseName) {
            this.baseName = baseName;
        }
    }

    private static final class SpikeEventSnapshot {
        private final int tick;
        private final long nanos;

        private SpikeEventSnapshot(int tick, long nanos) {
            this.tick = tick;
            this.nanos = nanos;
        }
    }

    private static final class SpikeBucketSnapshot {
        private final int startTick;
        private final int calls;
        private final long totalNanos;
        private final long maxNanos;

        private SpikeBucketSnapshot(int startTick, int calls, long totalNanos, long maxNanos) {
            this.startTick = startTick;
            this.calls = calls;
            this.totalNanos = totalNanos;
            this.maxNanos = maxNanos;
        }
    }

    private static final class DriveCoverageReportSnapshot {
        private final CompatTiming.DriveCoverageSnapshot resolver;
        private final int physicalDriveTargets;
        private final int activeDriveTargets;
        private final long exactTimedCalls;
        private final long extractCalls;
        private final long insertCalls;
        private final long preferredCalls;
        private final long availableStacksCalls;
        private final long otherCalls;

        private DriveCoverageReportSnapshot(CompatTiming.DriveCoverageSnapshot resolver,
                                            int physicalDriveTargets, int activeDriveTargets,
                                            long exactTimedCalls, long extractCalls, long insertCalls,
                                            long preferredCalls, long availableStacksCalls, long otherCalls) {
            this.resolver = resolver;
            this.physicalDriveTargets = physicalDriveTargets;
            this.activeDriveTargets = activeDriveTargets;
            this.exactTimedCalls = exactTimedCalls;
            this.extractCalls = extractCalls;
            this.insertCalls = insertCalls;
            this.preferredCalls = preferredCalls;
            this.availableStacksCalls = availableStacksCalls;
            this.otherCalls = otherCalls;
        }
    }

    private static final class PhysicalDeviceSnapshot {
        private final String dimension;
        private final BlockPos position;
        private final String type;
        private final long nanos;
        private final int calls;
        private final String gridLabel;
        private final PhysicalSpikeSnapshot spike;

        private PhysicalDeviceSnapshot(String dimension, BlockPos position, String type, long nanos, int calls,
                                       String gridLabel, PhysicalSpikeSnapshot spike) {
            this.dimension = dimension;
            this.position = position;
            this.type = type;
            this.nanos = nanos;
            this.calls = calls;
            this.gridLabel = gridLabel;
            this.spike = spike;
        }
    }

    private static final class DetailScopeState {
        private final int[] weights = new int[16];
        private int depth;

        private void push(int weight) {
            if (depth < weights.length) {
                weights[depth] = weight;
            }
            depth++;
        }

        private void pop() {
            if (depth > 0) depth--;
        }

        private int currentWeight() {
            if (depth <= 0) return 1;
            int index = Math.min(depth, weights.length) - 1;
            return weights[index];
        }
    }

    private static final class SamplingStats {
        private int currentTick = Integer.MIN_VALUE;
        private int observedThisTick;
        private int sampledThisTick;
        private int previousObserved;
        private int maxObservedPerTick;
        private int maxSampledPerTick;
        private long exactLifecycleCallbacks;
        private long levelLifecycleCallbacksSeen;
        private long levelLifecycleCallbacksSampled;
        private long skippedBySampling;
        private long skippedByBudget;
        private int maxSampleFactor = 1;
        private boolean largeServerMode;

        private void reset() {
            currentTick = Integer.MIN_VALUE; observedThisTick = 0; sampledThisTick = 0; previousObserved = 0;
            maxObservedPerTick = 0; maxSampledPerTick = 0; exactLifecycleCallbacks = 0L;
            levelLifecycleCallbacksSeen = 0L; levelLifecycleCallbacksSampled = 0L;
            skippedBySampling = 0L; skippedByBudget = 0L; maxSampleFactor = 1; largeServerMode = false;
        }

        private void recordExactLifecycle() {
            exactLifecycleCallbacks++;
        }

        private int admit(Object grid, Phase phase, Level level, int serverTick, int gridCount) {
            if (serverTick != Integer.MIN_VALUE && currentTick != serverTick) {
                if (currentTick != Integer.MIN_VALUE) {
                    previousObserved = observedThisTick;
                    maxObservedPerTick = Math.max(maxObservedPerTick, observedThisTick);
                    maxSampledPerTick = Math.max(maxSampledPerTick, sampledThisTick);
                }
                currentTick = serverTick; observedThisTick = 0; sampledThisTick = 0;
            }
            observedThisTick++; levelLifecycleCallbacksSeen++;
            if (gridCount <= LARGE_SERVER_EXACT_GRID_THRESHOLD) {
                levelLifecycleCallbacksSampled++; sampledThisTick++;
                return 1;
            }
            largeServerMode = true;
            int factor = factorForGridCount(gridCount);
            if (previousObserved > LARGE_SERVER_TARGET_DETAILED_CALLBACKS_PER_TICK) {
                factor = Math.max(factor, nextPowerOfTwo((previousObserved + LARGE_SERVER_TARGET_DETAILED_CALLBACKS_PER_TICK - 1)
                        / LARGE_SERVER_TARGET_DETAILED_CALLBACKS_PER_TICK));
            }
            factor = Math.min(LARGE_SERVER_MAX_SAMPLE_FACTOR, Math.max(1, factor));
            maxSampleFactor = Math.max(maxSampleFactor, factor);

            int levelHash = level == null ? 0 : System.identityHashCode(level);
            int hash = System.identityHashCode(grid) * 0x9e3779b9;
            hash ^= levelHash * 0x85ebca6b;
            hash ^= serverTick * 0xc2b2ae35;
            hash ^= phase.id * 0x27d4eb2d;
            hash ^= (hash >>> 16);
            if ((hash & (factor - 1)) != 0) {
                skippedBySampling++;
                return 0;
            }
            if (sampledThisTick >= LARGE_SERVER_HARD_DETAILED_CALLBACK_BUDGET_PER_TICK) {
                skippedByBudget++;
                return 0;
            }
            sampledThisTick++; levelLifecycleCallbacksSampled++;
            return factor;
        }

        private static int factorForGridCount(int grids) {
            if (grids <= 256) return 1;
            if (grids <= 512) return 2;
            if (grids <= 1024) return 4;
            if (grids <= 2048) return 8;
            if (grids <= 4096) return 16;
            if (grids <= 8192) return 32;
            return 64;
        }

        private static int nextPowerOfTwo(int value) {
            int x = 1;
            while (x < value && x < LARGE_SERVER_MAX_SAMPLE_FACTOR) x <<= 1;
            return x;
        }

        private SamplingSnapshot snapshot() {
            int observedMax = Math.max(maxObservedPerTick, observedThisTick);
            int sampledMax = Math.max(maxSampledPerTick, sampledThisTick);
            double effective = levelLifecycleCallbacksSampled <= 0 ? 1.0
                    : levelLifecycleCallbacksSeen / (double) levelLifecycleCallbacksSampled;
            return new SamplingSnapshot(largeServerMode, exactLifecycleCallbacks, levelLifecycleCallbacksSeen,
                    levelLifecycleCallbacksSampled, skippedBySampling, skippedByBudget, maxSampleFactor, effective,
                    observedMax, sampledMax);
        }
    }

    private static final class SamplingSnapshot {
        private final boolean largeServerMode;
        private final long exactLifecycleCallbacks;
        private final long levelCallbacksSeen;
        private final long levelCallbacksSampled;
        private final long skippedBySampling;
        private final long skippedByBudget;
        private final int maxSampleFactor;
        private final double effectiveSampleFactor;
        private final int maxObservedCallbacksPerTick;
        private final int maxDetailedCallbacksPerTick;
        private SamplingSnapshot(boolean largeServerMode, long exactLifecycleCallbacks, long levelCallbacksSeen,
                                 long levelCallbacksSampled, long skippedBySampling, long skippedByBudget,
                                 int maxSampleFactor, double effectiveSampleFactor, int maxObservedCallbacksPerTick,
                                 int maxDetailedCallbacksPerTick) {
            this.largeServerMode = largeServerMode; this.exactLifecycleCallbacks = exactLifecycleCallbacks;
            this.levelCallbacksSeen = levelCallbacksSeen; this.levelCallbacksSampled = levelCallbacksSampled;
            this.skippedBySampling = skippedBySampling; this.skippedByBudget = skippedByBudget;
            this.maxSampleFactor = maxSampleFactor; this.effectiveSampleFactor = effectiveSampleFactor;
            this.maxObservedCallbacksPerTick = maxObservedCallbacksPerTick;
            this.maxDetailedCallbacksPerTick = maxDetailedCallbacksPerTick;
        }
    }

    private static final class GridLifecycleToken {
        private final Object gridObject;
        private final GridInfo grid;
        private final Phase phase;
        private final int weight;
        private final long startedAt;
        private final long generation;

        private GridLifecycleToken(Object gridObject, GridInfo grid, Phase phase, int weight, long startedAt, long generation) {
            this.gridObject = gridObject;
            this.grid = grid;
            this.phase = phase;
            this.weight = weight;
            this.startedAt = startedAt;
            this.generation = generation;
        }
    }

    private static final class ServiceToken {
        private final GridInfo grid;
        private final ServiceInfo service;
        private final Phase phase;
        private final int dispatchLevelId;
        private final int weight;
        private final long startedAt;
        private final long generation;

        private ServiceToken(GridInfo grid, ServiceInfo service, Phase phase, int dispatchLevelId, int weight,
                             long startedAt, long generation) {
            this.grid = grid;
            this.service = service;
            this.phase = phase;
            this.dispatchLevelId = dispatchLevelId;
            this.weight = weight;
            this.startedAt = startedAt;
            this.generation = generation;
        }
    }

    private static final class TickManagerSectionToken {
        private final Object tickManager;
        private final GridInfo grid;
        private final TickSection section;
        private final int weight;
        private final long startedAt;
        private final long generation;

        private TickManagerSectionToken(Object tickManager, GridInfo grid, TickSection section, int weight,
                                        long startedAt, long generation) {
            this.tickManager = tickManager;
            this.grid = grid;
            this.section = section;
            this.weight = weight;
            this.startedAt = startedAt;
            this.generation = generation;
        }
    }

    private static final class TickQueueDetailToken {
        private final Object tickManager;
        private final GridInfo grid;
        private final int weight;
        private final long generation;
        private TickQueuePhase phase = TickQueuePhase.NONE;
        private long phaseStartedAt;

        private TickQueueDetailToken(Object tickManager, GridInfo grid, int weight, long generation) {
            this.tickManager = tickManager;
            this.grid = grid;
            this.weight = weight;
            this.generation = generation;
        }
    }

    private static final class TickManagerControlToken {
        private final Object tickManager;
        private final GridInfo grid;
        private final TickControl control;
        private final int weight;
        private final long startedAt;
        private final long generation;

        private TickManagerControlToken(Object tickManager, GridInfo grid, TickControl control, int weight,
                                        long startedAt, long generation) {
            this.tickManager = tickManager;
            this.grid = grid;
            this.control = control;
            this.weight = weight;
            this.startedAt = startedAt;
            this.generation = generation;
        }
    }

    private static final class DeviceToken {
        private final GridInfo grid;
        private final long startedAt;
        private final long generation;

        private DeviceToken(GridInfo grid, long startedAt, long generation) {
            this.grid = grid;
            this.startedAt = startedAt;
            this.generation = generation;
        }
    }

    private static final class TickManagerSnapshot {
        private final long serviceNanos;
        private final int serviceCalls;
        private final MetricSnapshot levelQueue;
        private final MetricSnapshot queue;
        private final long queueBookkeepingNanos;
        private final long queueResidualNanos;
        private final long levelDispatchNanos;
        private final long outerOverheadNanos;
        private final MetricSnapshot headDueCheck;
        private final MetricSnapshot dequeuePrep;
        private final MetricSnapshot rateUpdate;
        private final MetricSnapshot sleepBranch;
        private final MetricSnapshot awakeCheck;
        private final MetricSnapshot reinsert;
        private final MetricSnapshot futureStop;
        private final MetricSnapshot sleep;
        private final MetricSnapshot wake;
        private final MetricSnapshot alert;
        private final EnumMap<Modulation, Long> modulations;
        private final List<LevelDispatchSnapshot> dispatchLevels;
        private final long deviceNanos;
        private final int deviceCalls;

        private TickManagerSnapshot(long serviceNanos, int serviceCalls,
                                    MetricSnapshot levelQueue, MetricSnapshot queue,
                                    long queueBookkeepingNanos, long queueResidualNanos,
                                    long levelDispatchNanos, long outerOverheadNanos,
                                    MetricSnapshot headDueCheck, MetricSnapshot dequeuePrep,
                                    MetricSnapshot rateUpdate, MetricSnapshot sleepBranch,
                                    MetricSnapshot awakeCheck, MetricSnapshot reinsert, MetricSnapshot futureStop,
                                    MetricSnapshot sleep, MetricSnapshot wake, MetricSnapshot alert,
                                    EnumMap<Modulation, Long> modulations, List<LevelDispatchSnapshot> dispatchLevels,
                                    long deviceNanos, int deviceCalls) {
            this.serviceNanos = serviceNanos;
            this.serviceCalls = serviceCalls;
            this.levelQueue = levelQueue;
            this.queue = queue;
            this.queueBookkeepingNanos = queueBookkeepingNanos;
            this.queueResidualNanos = queueResidualNanos;
            this.levelDispatchNanos = levelDispatchNanos;
            this.outerOverheadNanos = outerOverheadNanos;
            this.headDueCheck = headDueCheck;
            this.dequeuePrep = dequeuePrep;
            this.rateUpdate = rateUpdate;
            this.sleepBranch = sleepBranch;
            this.awakeCheck = awakeCheck;
            this.reinsert = reinsert;
            this.futureStop = futureStop;
            this.sleep = sleep;
            this.wake = wake;
            this.alert = alert;
            this.modulations = modulations;
            this.dispatchLevels = dispatchLevels;
            this.deviceNanos = deviceNanos;
            this.deviceCalls = deviceCalls;
        }

        private boolean hasInternalData() {
            return levelQueue.nanos > 0L || queue.nanos > 0L || hasQueuePhaseData()
                    || hasControlData() || !modulations.isEmpty();
        }

        private boolean hasQueuePhaseData() {
            return headDueCheck.nanos > 0L || dequeuePrep.nanos > 0L || rateUpdate.nanos > 0L
                    || sleepBranch.nanos > 0L || awakeCheck.nanos > 0L || reinsert.nanos > 0L
                    || futureStop.nanos > 0L;
        }

        private boolean hasControlData() {
            return sleep.nanos > 0L || wake.nanos > 0L || alert.nanos > 0L;
        }
    }

    private static final class LevelDispatchSnapshot {
        private final String dimension;
        private final boolean anchorDimension;
        private final MetricSnapshot total;
        private final MetricSnapshot levelStart;
        private final MetricSnapshot levelEnd;

        private LevelDispatchSnapshot(String dimension, boolean anchorDimension, MetricSnapshot total,
                                      MetricSnapshot levelStart, MetricSnapshot levelEnd) {
            this.dimension = dimension;
            this.anchorDimension = anchorDimension;
            this.total = total;
            this.levelStart = levelStart;
            this.levelEnd = levelEnd;
        }
    }

    private static final class ServiceSnapshot {
        private final String className;
        private final String displayName;
        private final KnownService known;
        private final MetricSnapshot total;
        private final EnumMap<Phase, MetricSnapshot> phases;

        private ServiceSnapshot(String className, String displayName, KnownService known,
                                MetricSnapshot total, EnumMap<Phase, MetricSnapshot> phases) {
            this.className = className;
            this.displayName = displayName;
            this.known = known;
            this.total = total;
            this.phases = phases;
        }
    }

    private static final class GridSnapshot {
        private final String label;
        private final BlockPos anchor;
        private final BlockEntity anchorEntity;
        private final BlockPos virtualBase;
        private final long gridCoreNanos;
        private final int gridCoreCalls;
        private final EnumMap<Phase, MetricSnapshot> gridPhases;
        private final long serviceTotalNanos;
        private final int serviceTotalCalls;
        private final long gridOverheadNanos;
        private final long deviceNanos;
        private final int deviceCalls;
        private final List<ServiceSnapshot> services;
        private final TickManagerSnapshot tickManager;
        private final long scoreNanos;
        private final boolean runtimeDetailed;

        private GridSnapshot(String label, BlockPos anchor, BlockEntity anchorEntity, BlockPos virtualBase,
                             long gridCoreNanos, int gridCoreCalls,
                             EnumMap<Phase, MetricSnapshot> gridPhases,
                             long serviceTotalNanos, int serviceTotalCalls,
                             long gridOverheadNanos,
                             long deviceNanos, int deviceCalls,
                             List<ServiceSnapshot> services, TickManagerSnapshot tickManager, long scoreNanos,
                             boolean runtimeDetailed) {
            this.label = label;
            this.anchor = anchor;
            this.anchorEntity = anchorEntity;
            this.virtualBase = virtualBase;
            this.gridCoreNanos = gridCoreNanos;
            this.gridCoreCalls = gridCoreCalls;
            this.gridPhases = gridPhases;
            this.serviceTotalNanos = serviceTotalNanos;
            this.serviceTotalCalls = serviceTotalCalls;
            this.gridOverheadNanos = gridOverheadNanos;
            this.deviceNanos = deviceNanos;
            this.deviceCalls = deviceCalls;
            this.services = services;
            this.tickManager = tickManager;
            this.scoreNanos = scoreNanos;
            this.runtimeDetailed = runtimeDetailed;
        }
    }
}
