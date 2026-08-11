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
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Per-grid AE2 profiler used by the Forge compatibility layer.
 *
 * v20.1 is a dual-view monitoring dashboard release. It keeps every v16 diagnostic counter,
 * keeps the physical AE2 timing-target registry and adds a plain-language moderator view plus the full expert view.
 * Compare keeps stable network identity across anchor changes and duplicate anchors: only unique dimension+anchor pairs are matched directly; ambiguous/changed anchors use a high-confidence, unambiguous physical-device fingerprint fallback.
 * Operator navigation remains:
 * device-to-grid links, per-call cost, copyable coordinates/teleport commands,
 * Top devices inside a selected Grid, dimension summaries and collapsed diagnostics. No AE2 scheduling or
 * game behavior is changed. The client still receives one inclusive total marker
 * per AE2 grid.
 *
 * Grid Core is inclusive. Grid Services is the sum of the service calls made by
 * Grid. Grid overhead is derived as Grid Core - Grid Services. Devices and the
 * TickManager sub-metrics are nested and must not be added to the inclusive
 * totals.
 */
public final class AE2GridProfiler {
    private static final Logger LOGGER = LogManager.getLogger("Observable/AE2Grid");
    private static final DateTimeFormatter REPORT_FILE_TIME = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");
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

    private static final ThreadLocal<ArrayDeque<GridLifecycleToken>> GRID_LIFECYCLE_STACK =
            new ThreadLocal<ArrayDeque<GridLifecycleToken>>() {
                @Override
                protected ArrayDeque<GridLifecycleToken> initialValue() {
                    return new ArrayDeque<>();
                }
            };
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

    private static final int FIXED_VIRTUAL_MARKERS = 17;
    private static final int MAX_SERVICE_MARKERS = 20;
    private static final int MAX_VIRTUAL_MARKERS = FIXED_VIRTUAL_MARKERS + MAX_SERVICE_MARKERS;

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
            sessionGeneration++;
            sessionActive = true;
        }
        GRID_LIFECYCLE_STACK.remove();
        DEVICE_TOKEN.remove();
        TICK_MANAGER_SECTION_STACK.remove();
        TICK_QUEUE_DETAIL_STACK.remove();
        TICK_MANAGER_CONTROL_STACK.remove();
        DISPATCH_LEVEL_CACHE.remove();
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
            PhysicalDeviceRef ref = new PhysicalDeviceRef(
                    host, dimension, new BlockPos(pos.getX(), pos.getY(), pos.getZ()), name);
            PHYSICAL_DEVICE_HOSTS.put(host, ref);
            PHYSICAL_DEVICES.put(key, ref);
        }
    }

    private static String physicalDeviceKey(String dimension, BlockPos pos) {
        return dimension + "|" + pos.getX() + "," + pos.getY() + "," + pos.getZ();
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
        DEVICE_TOKEN.set(new DeviceToken(info, System.nanoTime(), sessionGeneration));
    }

    public static void endDevice() {
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
        if (Props.notProcessing || !sessionActive || tickManager == null || !isServerProfilerThread()) {
            return;
        }
        TickSection section = TickSection.fromId(sectionId);
        if (section == null) {
            return;
        }
        GridInfo info = findGridForTickManager(tickManager);
        if (info == null) {
            return;
        }
        TICK_MANAGER_SECTION_STACK.get().push(
                new TickManagerSectionToken(tickManager, info, section, System.nanoTime(), sessionGeneration));
    }

    /** Finish an internal TickManagerService section. */
    public static void endTickManagerSection(Object tickManager, int sectionId) {
        long finishedAt = System.nanoTime();
        TickSection section = TickSection.fromId(sectionId);
        if (tickManager == null || section == null) {
            return;
        }

        TickManagerSectionToken token = removeMatchingSectionToken(tickManager, section);
        if (token == null || Props.notProcessing || !sessionActive || token.generation != sessionGeneration) {
            return;
        }

        synchronized (token.grid) {
            Metric metric = token.section == TickSection.LEVEL_QUEUE
                    ? token.grid.tickManager.levelQueue
                    : token.grid.tickManager.queue;
            recordLocked(metric, finishedAt - token.startedAt);
        }
    }

    /**
     * Starts v10's fine-grained state machine for one tickQueue invocation. The
     * state machine intentionally uses only phase boundaries supplied by the
     * mixin; it never reflects into PriorityQueue/TickTracker on the hot path.
     */
    public static void beginTickQueueDetail(Object tickManager) {
        if (Props.notProcessing || !sessionActive || tickManager == null || !isServerProfilerThread()) {
            return;
        }
        GridInfo info = findGridForTickManager(tickManager);
        if (info == null) {
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
        stack.push(new TickQueueDetailToken(tickManager, info, sessionGeneration));
    }

    /** Called immediately before PriorityQueue.peek(). */
    public static void tickQueueHeadCheck(Object tickManager) {
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
        TickQueueDetailToken token = findTickQueueDetailToken(tickManager);
        if (!validTickQueueToken(token)) {
            return;
        }
        token.phase = TickQueuePhase.RATE_UPDATE;
        token.phaseStartedAt = System.nanoTime();
    }

    /** Enters either the SLEEP transition or the awake-check/requeue branch. */
    public static void tickQueueBranch(Object tickManager, int branchId) {
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
        long now = System.nanoTime();
        TickQueueDetailToken token = removeTickQueueDetailToken(tickManager);
        if (!validTickQueueToken(token)) {
            return;
        }
        closeTickQueuePhase(token, now, true);
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
            recordLocked(metric, elapsed);
        }
    }

    /** Begin measuring ITickManager sleep/wake/alert bookkeeping. */
    public static void beginTickManagerControl(Object tickManager, int controlId) {
        if (Props.notProcessing || !sessionActive || tickManager == null || !isServerProfilerThread()) {
            return;
        }
        TickControl control = TickControl.fromId(controlId);
        if (control == null) {
            return;
        }
        GridInfo info = findGridForTickManager(tickManager);
        if (info == null) {
            return;
        }
        TICK_MANAGER_CONTROL_STACK.get().push(
                new TickManagerControlToken(tickManager, info, control, System.nanoTime(), sessionGeneration));
    }

    /** Finish measuring ITickManager sleep/wake/alert bookkeeping. */
    public static void endTickManagerControl(Object tickManager, int controlId) {
        long finishedAt = System.nanoTime();
        TickControl control = TickControl.fromId(controlId);
        if (tickManager == null || control == null) {
            return;
        }

        TickManagerControlToken token = removeMatchingControlToken(tickManager, control);
        if (token == null || Props.notProcessing || !sessionActive || token.generation != sessionGeneration) {
            return;
        }

        synchronized (token.grid) {
            recordLocked(controlMetric(token.grid.tickManager, control), finishedAt - token.startedAt);
        }
    }

    /** Called at HEAD of one of Grid's four lifecycle dispatch methods. */
    public static void beginGridLifecycle(Object grid, int phaseId) {
        if (Props.notProcessing || !sessionActive || !isGridObject(grid) || !isServerProfilerThread()) {
            return;
        }

        Phase phase = Phase.fromId(phaseId);
        if (phase == null) {
            return;
        }

        GridInfo info = getOrCreateGrid(grid);
        ensureAnchor(info, null);
        GRID_LIFECYCLE_STACK.get().push(
                new GridLifecycleToken(grid, info, phase, System.nanoTime(), sessionGeneration));
    }

    /** Called at RETURN of one of Grid's four lifecycle dispatch methods. */
    public static void endGridLifecycle(Object grid, int phaseId) {
        long finishedAt = System.nanoTime();
        if (grid == null) {
            return;
        }

        Phase phase = Phase.fromId(phaseId);
        ArrayDeque<GridLifecycleToken> stack = GRID_LIFECYCLE_STACK.get();
        GridLifecycleToken token = null;

        if (!stack.isEmpty() && stack.peek().gridObject == grid && stack.peek().phase == phase) {
            token = stack.pop();
        } else if (!stack.isEmpty()) {
            Iterator<GridLifecycleToken> iterator = stack.iterator();
            while (iterator.hasNext()) {
                GridLifecycleToken candidate = iterator.next();
                if (candidate.gridObject == grid && candidate.phase == phase) {
                    token = candidate;
                    iterator.remove();
                    break;
                }
            }
        }

        if (token == null || Props.notProcessing || !sessionActive || token.generation != sessionGeneration) {
            return;
        }

        long elapsed = finishedAt - token.startedAt;
        synchronized (token.grid) {
            recordLocked(token.grid.gridCore, elapsed);
            recordLocked(token.grid.gridPhases.get(token.phase), elapsed);
        }
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

    /**
     * Called immediately before Grid invokes one IGridServiceProvider lifecycle
     * method. The returned token is intentionally opaque to keep the mixin free
     * of collector implementation details.
     */
    public static Object beginService(Object grid, Object service, int phaseId) {
        return beginService(grid, service, phaseId, null);
    }

    /**
     * Level-aware overload used by Grid.onLevelStartTick/onLevelEndTick. Keeping
     * the Level object itself as the hot-path key avoids allocating dimension
     * strings tens of thousands of times during large server profiles.
     */
    public static Object beginService(Object grid, Object service, int phaseId, Level level) {
        if (Props.notProcessing || !sessionActive || !isGridObject(grid)
                || service == null || !isServerProfilerThread()) {
            return null;
        }

        Phase phase = Phase.fromId(phaseId);
        if (phase == null) {
            return null;
        }

        GridInfo info = getOrCreateGrid(grid);
        ensureAnchor(info, null);
        ServiceInfo serviceInfo = getOrCreateService(info, service);
        int dispatchLevelId = -1;
        if (serviceInfo.known == KnownService.TICK_MANAGER && level != null
                && (phase == Phase.LEVEL_START || phase == Phase.LEVEL_END)) {
            dispatchLevelId = dispatchLevelId(level);
        }

        // Start the clock only after all profiler bookkeeping above. This keeps
        // the measured service time as close as possible to the actual call.
        return new ServiceToken(info, serviceInfo, phase, dispatchLevelId, System.nanoTime(), sessionGeneration);
    }

    /** Called immediately after the service method returns (also from finally). */
    public static void endService(Object opaqueToken) {
        long finishedAt = System.nanoTime();
        if (!(opaqueToken instanceof ServiceToken)) {
            return;
        }

        ServiceToken token = (ServiceToken) opaqueToken;
        if (Props.notProcessing || !sessionActive || token.generation != sessionGeneration) {
            return;
        }

        long elapsed = finishedAt - token.startedAt;
        synchronized (token.grid) {
            recordLocked(token.service.total, elapsed);
            recordLocked(token.service.phases.get(token.phase), elapsed);

            // v16: prove where high TickManager level-dispatch fan-out comes
            // from. The Level is already reduced to a compact integer ID before
            // timing starts; no extra nanoTime or per-dispatch string allocation
            // is added on this hot path.
            if (token.dispatchLevelId >= 0
                    && (token.phase == Phase.LEVEL_START || token.phase == Phase.LEVEL_END)) {
                LevelDispatchInfo dispatch = token.grid.tickManager.dispatchFor(token.dispatchLevelId);
                recordLocked(token.phase == Phase.LEVEL_START ? dispatch.levelStart : dispatch.levelEnd, elapsed);
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

        List<GridSnapshot> snapshots = snapshotsSorted();
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

    /**
     * Writes the complete AE2 grid breakdown to a standalone JSON file. This is
     * intentionally separate from Observable's own uploaded profile: the file
     * is easier to archive/compare while the uploaded profile keeps the same
     * virtual metrics for the web visualizer.
     */
    public static Path writeDetailedReport(int profileTicks) {
        if (profileTicks <= 0) {
            return null;
        }

        try {
            List<GridSnapshot> snapshots = snapshotsSorted();
            List<PhysicalDeviceSnapshot> physicalDevices = physicalDevicesSorted();
            Path directory = FMLPaths.GAMEDIR.get().resolve("observable-reports");
            Files.createDirectories(directory);

            String baseName = "ae2-grid-" + REPORT_FILE_TIME.format(LocalDateTime.now());
            Path jsonFile = directory.resolve(baseName + ".json");
            Path htmlFile = directory.resolve(baseName + ".html");

            String json = buildDetailedReportJson(snapshots, physicalDevices, profileTicks);
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
            return jsonFile;
        } catch (Throwable t) {
            // Reporting must never make a completed profile fail.
            LOGGER.warn("Failed to write Observable AE2 detailed report", t);
            return null;
        }
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
                result.add(new PhysicalDeviceSnapshot(
                        dimension, new BlockPos(timingEntry.getKey().getX(), timingEntry.getKey().getY(),
                        timingEntry.getKey().getZ()), name, nanos, calls, gridLabel));
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
<title>Observable AE2 Monitoring v20.1</title>
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
.badge{display:inline-block;min-width:44px;text-align:center;border:1px solid var(--line);padding:2px 6px;border-radius:999px;font-size:11px}.sev-hot{color:var(--hot);border-color:#7c3f50;background:#2a1720}.sev-warn{color:var(--warn);border-color:#665b32;background:#211f16}.sev-mid{color:var(--accent);border-color:#285f6c;background:#102329}.sev-low{color:var(--low);border-color:#354d76;background:#111b2b}
.summary-wrap{display:grid;grid-template-columns:minmax(0,1.3fr) minmax(330px,.7fr);gap:12px;margin:12px 0}.selection-panel .big{font-size:28px;font-weight:700;margin:3px 0}.kv{display:grid;grid-template-columns:1fr auto;gap:5px 10px;margin-top:8px}.kv span:nth-child(odd){color:var(--muted)}
.row{display:grid;grid-template-columns:minmax(190px,1.1fr) minmax(220px,2.8fr) 115px 105px;gap:10px;align-items:center;padding:7px 0;border-top:1px solid rgba(38,52,71,.55)}.row:first-of-type{border-top:0}.label{white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.barbox{height:13px;background:#0d131b;border-radius:99px;overflow:hidden;border:1px solid #202c3a}.bar{height:100%;min-width:0;background:linear-gradient(90deg,var(--accent),var(--accent2));border-radius:99px}.value{text-align:right;font-variant-numeric:tabular-nums}.calls{text-align:right;color:var(--muted);font-variant-numeric:tabular-nums}.indent1 .label{padding-left:16px}.indent2 .label{padding-left:32px}.hot .bar{background:linear-gradient(90deg,var(--warn),var(--hot))}
.meta{display:flex;gap:15px;flex-wrap:wrap;color:var(--muted);margin:5px 0 10px}.empty{color:var(--muted);padding:8px 0}.mods{display:grid;grid-template-columns:repeat(3,minmax(150px,1fr));gap:8px}.mod{background:var(--panel2);border:1px solid var(--line);border-radius:9px;padding:10px}.mod strong{font-size:17px;display:block}
details.advanced{padding:0;overflow:hidden}details.advanced>summary{cursor:pointer;padding:13px 14px;font-weight:650;background:var(--panel2);list-style:none}details.advanced>summary::-webkit-details-marker{display:none}details.advanced>summary:before{content:'▶';display:inline-block;margin-right:8px;color:var(--accent);transition:transform .12s}details.advanced[open]>summary:before{transform:rotate(90deg)}.advanced-body{padding:2px 13px 13px}.advanced-body section{background:#0f161f;margin:10px 0}.dispatch-scroll{max-height:390px;overflow:auto;border:1px solid var(--line);border-radius:9px}.foreign{color:var(--warn)}.anchor{color:var(--good)}
.simple-hero{background:linear-gradient(135deg,#102329,#161d2a);border:1px solid #285f6c;border-radius:14px;padding:18px;margin:12px 0}.simple-hero h2{font-size:21px;margin:0 0 7px}.simple-hero .big{font-size:30px;font-weight:750;margin:4px 0}.simple-hero .lead{font-size:15px;max-width:950px}.simple-issues{display:grid;grid-template-columns:repeat(2,minmax(300px,1fr));gap:10px}.simple-issue{background:var(--panel2);border:1px solid var(--line);border-radius:11px;padding:12px}.simple-issue.hot{border-color:#7c3f50}.simple-issue h3{font-size:15px;margin:0 0 5px}.simple-load{font-size:22px;font-weight:700}.simple-why{margin:6px 0;color:var(--muted)}.simple-actions{display:flex;gap:6px;flex-wrap:wrap;margin-top:8px}.simple-actions button{appearance:none;border:1px solid var(--line);background:#101923;color:var(--text);padding:5px 8px;border-radius:7px;cursor:pointer}.simple-actions button:hover{border-color:var(--accent)}.plain-good{color:var(--good)}.plain-warn{color:var(--warn)}.plain-hot{color:var(--hot)}.simple-list{display:grid;gap:8px}.simple-device{display:grid;grid-template-columns:minmax(210px,1.4fr) 115px minmax(210px,1.3fr) minmax(230px,1.6fr);gap:10px;align-items:center;border-top:1px solid rgba(38,52,71,.6);padding:9px 0}.simple-device:first-child{border-top:0}.simple-device .where{color:var(--muted)}.simple-compare-summary{font-size:16px;margin:8px 0 12px}.simple-shell.hidden,.expert-shell.hidden{display:none}.mode-hint{color:var(--muted);font-size:12px;margin-top:4px}.simple-explain{display:grid;grid-template-columns:repeat(3,minmax(220px,1fr));gap:8px}.simple-explain>div{background:#0f161f;border:1px solid var(--line);border-radius:9px;padding:10px}.type-grid{display:grid;grid-template-columns:repeat(3,minmax(180px,1fr));gap:8px;margin-bottom:12px}.type-card{background:var(--panel2);border:1px solid var(--line);border-radius:9px;padding:10px}.type-card strong{display:block;font-size:17px;margin:2px 0}.type-card small{color:var(--muted)}.mini-actions{display:flex;gap:5px;justify-content:flex-end;flex-wrap:wrap}.mini-btn,.grid-link{appearance:none;border:1px solid var(--line);background:#101923;color:var(--text);padding:3px 7px;border-radius:6px;cursor:pointer;font-size:11px}.mini-btn:hover,.grid-link:hover{border-color:var(--accent)}.grid-link{color:var(--accent);white-space:nowrap}.coord-line{display:flex;gap:8px;align-items:center;flex-wrap:wrap}.compare-toolbar{display:grid;grid-template-columns:minmax(230px,1.5fr) 170px 130px 130px 150px;gap:8px;align-items:end}.drop-zone{border:1px dashed #4b6688;border-radius:10px;padding:14px;background:#0d131b}.drop-zone.drag{border-color:var(--accent);background:#102329}.delta-up{color:var(--hot);font-weight:650}.delta-down{color:var(--good);font-weight:650}.delta-new{color:var(--warn);font-weight:650}.delta-gone{color:var(--muted);font-weight:650}.status-pill{display:inline-block;border:1px solid var(--line);border-radius:999px;padding:2px 7px;font-size:11px}.compare-help{display:flex;gap:14px;flex-wrap:wrap;margin-top:8px}.footer{margin-top:22px;color:var(--muted);font-size:12px}
@media(max-width:1100px){.cards{grid-template-columns:repeat(3,1fr)}.toolbar,.device-toolbar,.compare-toolbar{grid-template-columns:repeat(3,minmax(150px,1fr))}.dashboard-grid,.summary-wrap{grid-template-columns:1fr}.type-grid{grid-template-columns:repeat(2,1fr)}.simple-issues{grid-template-columns:1fr}.simple-device{grid-template-columns:1fr 110px 1fr}.simple-device .simple-actions{grid-column:1/4}.simple-explain{grid-template-columns:1fr}}
@media(max-width:720px){.cards{grid-template-columns:repeat(2,1fr)}.toolbar,.device-toolbar,.compare-toolbar{grid-template-columns:1fr 1fr}.row{grid-template-columns:1fr 90px}.barbox{grid-column:1/3;grid-row:2}.calls{display:none}.mods,.type-grid{grid-template-columns:1fr}.data-table{min-width:760px}.simple-device{grid-template-columns:1fr}.simple-device .simple-actions{grid-column:auto}.simple-hero .big{font-size:24px}}
</style>
</head>
<body>
<main>
<div class="top"><div><h1>Observable · AE2 Monitoring</h1><div id="reportMeta" class="meta"></div></div><div class="actions"><button id="simpleModeBtn" class="active">Простой режим</button><button id="expertModeBtn">Экспертный</button><button id="jsonBtn">Открыть JSON</button><button id="copyBtn">Копировать сводку</button></div></div>
<div id="simpleShell" class="simple-shell">
 <div id="simpleHero" class="simple-hero"></div>
 <div id="simpleCards" class="cards"></div>
 <div class="dashboard-grid">
  <section><div class="panel-head"><h2>Где лагает сильнее всего</h2><span class="muted">сети AE2 простыми словами</span></div><div id="simpleProblems" class="simple-issues"></div></section>
  <section><div class="panel-head"><h2>Самые тяжёлые dimensions</h2><span class="muted">сумма AE2 Grid Core</span></div><div id="simpleDimensions"></div></section>
 </div>
 <section><div class="panel-head"><h2>Куда телепортироваться сначала</h2><span class="muted">конкретные физические устройства</span></div><div id="simpleDevices" class="simple-list"></div></section>
 <section>
  <div class="panel-head"><h2>Сравнить с прошлым профилем</h2><span class="muted">покажет, что стало хуже или лучше</span></div>
  <div class="drop-zone"><b>Выбери прошлый ae2-grid-....json</b><div class="muted">Файл читается только в браузере и никуда не отправляется.</div><input id="simpleCompareFile" type="file" accept=".json,application/json" style="margin-top:10px"></div>
  <div id="simpleCompareMeta" class="meta"></div><div id="simpleCompare"></div>
 </section>
 <details class="advanced"><summary>Что означают эти цифры</summary><div class="advanced-body"><div class="simple-explain"><div><b>µs/t</b><br><span class="muted">Сколько микросекунд среднего серверного тика занял объект. 1000 µs/t = 1 ms/t.</span></div><div><b>Avg / call</b><br><span class="muted">Сколько в среднем стоит одно срабатывание устройства. Полезно для редких, но очень тяжёлых операций.</span></div><div><b>50 ms budget</b><br><span class="muted">При 20 TPS один серверный тик имеет бюджет 50 ms. Это только доля измеренной AE2-нагрузки, а не полный MSPT сервера.</span></div></div></div></details>
</div>

<div id="expertShell" class="expert-shell hidden">
<div class="nav-tabs"><button data-view="overview" class="active">Hotspots</button><button data-view="grids">AE2 Grids</button><button data-view="devices">Physical devices</button><button data-view="compare">Compare profiles</button></div>
<div class="note">Экспертный режим: отчёт ничего не оптимизирует и не меняет в AE2. Inclusive/nested значения внутри одной Grid не складываются. Hotspots показывает текущую нагрузку, Compare profiles локально сравнивает этот отчёт с выбранным JSON. Данные никуда не отправляются.</div>

<div id="page-overview" class="page">
 <div id="globalCards" class="cards"></div>
 <div class="dashboard-grid">
  <div class="panel"><div class="panel-head"><h2>Top AE2 Grids</h2><span class="muted">клик → открыть Grid</span></div><div id="topGrids"></div></div>
  <div class="panel"><div class="panel-head"><h2>Dimensions</h2><span class="muted">Σ Grid Core</span></div><div id="dimensionSummary"></div></div>
 </div>
 <section><div class="panel-head"><h2>Top physical AE2 devices</h2><span class="muted">отдельные timing buckets; не прибавлять к Grid Core</span></div><div id="topDevices"></div></section>
  <div class="dashboard-grid">
   <div class="panel"><div class="panel-head"><h2>Physical load by Grid</h2><span class="muted">Σ physical timing targets</span></div><div id="physicalByGrid"></div></div>
   <div class="panel"><div class="panel-head"><h2>Device types</h2><span class="muted">Σ load / count / avg call</span></div><div id="hotspotTypes"></div></div>
  </div>
</div>

<div id="page-grids" class="page hidden">
 <div class="toolbar">
  <div class="field"><label for="gridSearch">Поиск Grid / dimension / координаты</label><input id="gridSearch" type="search" placeholder="например: #82, overworld, ps_adobeaudition, -23 64 6"></div>
  <div class="field"><label for="gridDimension">Dimension</label><select id="gridDimension"></select></div>
  <div class="field"><label for="gridMin">Минимум Core, µs/t</label><input id="gridMin" type="number" min="0" step="0.1" value="5"></div>
  <div class="field"><label for="gridSort">Сортировка</label><select id="gridSort"><option value="core">Grid Core</option><option value="devices">Devices</option><option value="scheduler">Scheduler</option><option value="overhead">Grid overhead</option><option value="services">Services</option><option value="dispatch">Level dispatch</option><option value="foreign">Foreign dispatch</option></select></div>
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
  <div class="field"><label for="deviceSort">Сортировка</label><select id="deviceSort"><option value="time">Нагрузка</option><option value="avg">Avg / call</option><option value="calls">Calls/t</option><option value="type">Тип</option></select></div>
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
 <section><div class="panel-head"><h2>AE2 Grid changes</h2><span class="muted">match: unique anchor first, duplicate/changed anchor → device fingerprint</span></div><div id="compareGridList"><div class="empty">Выбери baseline JSON.</div></div></section>
 <section><div class="panel-head"><h2>Physical device changes</h2><span class="muted">match: dimension + type + coordinates</span></div><div id="compareDeviceList"><div class="empty">Выбери baseline JSON.</div></div></section>
</div>
</div>

<div class="footer">Observable AE2 v20.1 dual-view monitoring. Compare treats only unique dimension + anchor pairs as stable IDs; duplicate or changed anchors fall back to an unambiguous physical-device fingerprint. Moderator TP uses /observable tp.</div>
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
const n=v=>Number.isFinite(Number(v))?Number(v):0;
const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const fmt=v=>{v=n(v);return `${v>=100?v.toFixed(1):v>=10?v.toFixed(2):v.toFixed(3)} µs/t`};
const calls=v=>`${n(v)>=10?n(v).toFixed(1):n(v).toFixed(3)}x/t`;
const avgCallUs=m=>n(m?.usPerCall)||(n(m?.calls)>0?n(m?.totalNanos)/1000/n(m?.calls):0);
const fmtCall=v=>{v=n(v);return v>=1000?`${(v/1000).toFixed(v>=10000?1:2)} ms/call`:`${v>=100?v.toFixed(1):v>=10?v.toFixed(2):v.toFixed(3)} µs/call`};
const dimShort=d=>{d=String(d||'unknown');const p=d.lastIndexOf('/');return p>=0?d.slice(p+1):d};
const posText=p=>p?`${p.x} ${p.y} ${p.z}`:'';
const anchorText=g=>posText(g?.anchor);
const gridByLabel=label=>allGrids.find(g=>g.label===label)||null;
const tpCommand=(dimension,p)=>p?`/observable tp ${dimension} position ${n(p.x)} ${n(p.y)} ${n(p.z)}`:'';
const coordActions=(dimension,p)=>p?`<span class="mini-actions"><button class="mini-btn" data-copy-text="${esc(posText(p))}">Copy coords</button><button class="mini-btn" data-copy-text="${esc(tpCommand(dimension,p))}">Copy TP</button></span>`:'';
const gridLink=label=>label?`<button class="grid-link" data-open-grid="${esc(label)}">${esc(label)}</button>`:'<span class="muted">unlinked</span>';
const dispatchTotal=g=>(g?.tickManager?.dispatchLevels||[]).reduce((a,x)=>a+n(x.total?.usPerTick),0);
const foreignDispatch=g=>(g?.tickManager?.dispatchLevels||[]).filter(x=>!x.anchorDimension).reduce((a,x)=>a+n(x.total?.usPerTick),0);
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
const UI_STORAGE_KEY='observable-ae2-v20-ui';

function switchView(view,persist=true){currentView=view;document.querySelectorAll('.page').forEach(p=>p.classList.toggle('hidden',p.id!==`page-${view}`));document.querySelectorAll('.nav-tabs button').forEach(b=>b.classList.toggle('active',b.dataset.view===view));if(view==='overview')renderOverview();if(view==='grids')applyGridFilters(false);if(view==='devices')applyDeviceFilters(false);if(view==='compare')renderCompare();if(persist)saveUiState()}
function setSiteMode(mode,persist=true){siteMode=mode==='expert'?'expert':'simple';document.getElementById('simpleShell').classList.toggle('hidden',siteMode!=='simple');document.getElementById('expertShell').classList.toggle('hidden',siteMode!=='expert');document.getElementById('simpleModeBtn').classList.toggle('active',siteMode==='simple');document.getElementById('expertModeBtn').classList.toggle('active',siteMode==='expert');if(siteMode==='simple')renderSimple();else switchView(currentView,false);if(persist)saveUiState()}
function populateDimensionSelect(id,values){const el=document.getElementById(id);el.innerHTML='<option value="*">Все dimensions</option>'+values.map(d=>`<option value="${esc(d)}">${esc(dimShort(d))}</option>`).join('')}
const pctText=(v,total)=>total>0?`${(n(v)/total*100).toFixed(n(v)/total*100>=10?1:2)}%`:'—';
const gridKey=g=>`${g?.dimension||'unknown'}|${n(g?.anchor?.x)},${n(g?.anchor?.y)},${n(g?.anchor?.z)}`;
const deviceKey=d=>`${d?.dimension||'unknown'}|${d?.type||'unknown'}|${n(d?.position?.x)},${n(d?.position?.y)},${n(d?.position?.z)}`;
const HUMAN_TYPES={'ae2:export_bus':'Шина экспорта AE2','ae2:import_bus':'Шина импорта AE2','ae2:storage_bus':'Шина хранения AE2','ae2:drive':'ME Drive','ae2:charger':'Зарядник AE2','ae2:dense_energy_cell':'Плотная энергетическая ячейка','ae2:energy_cell':'Энергетическая ячейка','expatternprovider:tag_export_bus':'Tag Export Bus','expatternprovider:ex_export_bus_part':'Extended Export Bus','expatternprovider:ex_import_bus_part':'Extended Import Bus','expatternprovider:oversize_interface':'Oversize Interface'};
const humanType=t=>HUMAN_TYPES[t]||String(t||'unknown').replace(/^ae2:/,'AE2 ').replace(/^expatternprovider:/,'ExtendedAE ').replace(/_/g,' ');
const ms=v=>`${(n(v)/1000).toFixed(n(v)>=10000?1:2)} ms/t`;
const tickBudgetPct=v=>n(v)/50000*100;
function simpleGrade(v){const p=tickBudgetPct(v);return p>=10?['Высокая нагрузка','plain-hot']:p>=4?['Заметная нагрузка','plain-warn']:['Умеренная нагрузка','plain-good']}
function physicalForGrid(label,devices=allDevices){return devices.filter(d=>d.gridLabel===label).sort((a,b)=>n(b.metric?.usPerTick)-n(a.metric?.usPerTick))}
function deviceFrequencyText(d){const c=n(d?.metric?.callsPerTick),avg=avgCallUs(d?.metric);if(c<=0)return 'За этот профиль не срабатывало.';if(c<0.05&&avg>=1000)return `Срабатывает редко, но один вызов тяжёлый: ${fmtCall(avg)}.`;if(c>=0.8)return `Работает почти каждый тик; одно срабатывание ${fmtCall(avg)}.`;if(c<0.1)return `Срабатывает примерно раз в ${Math.max(1,Math.round(1/c))} тиков; одно срабатывание ${fmtCall(avg)}.`;return `Срабатывает ${calls(c)}; одно срабатывание ${fmtCall(avg)}.`}
function simpleGridCause(g){const core=valueOf(g,'core'),devices=valueOf(g,'devices'),scheduler=valueOf(g,'scheduler'),overhead=valueOf(g,'overhead');const top=[['работа устройств сети',devices],['планировщик AE2',scheduler],['служебная работа самой Grid',overhead]].sort((a,b)=>b[1]-a[1])[0];const pd=physicalForGrid(g.label)[0];let text=`Основная измеренная часть — ${top[0]} (${fmt(top[1])}).`;if(pd&&n(pd.metric?.usPerTick)>=1)text+=` Самый тяжёлый физический объект: ${humanType(pd.type)} (${fmt(pd.metric?.usPerTick)}).`;return text}

function physicalByGridStats(){const map=new Map();for(const d of allDevices){const k=d.gridLabel||'(unlinked)';let x=map.get(k);if(!x){x={gridLabel:k,count:0,total:0,max:0,top:null};map.set(k,x)}const v=n(d.metric?.usPerTick);x.count++;x.total+=v;if(v>x.max){x.max=v;x.top=d}}return [...map.values()].sort((a,b)=>b.total-a.total)}
function deviceTypeStats(devices=allDevices){const map=new Map();for(const d of devices){const k=d.type||'unknown';let x=map.get(k);if(!x){x={type:k,count:0,total:0,max:0,nanos:0,calls:0};map.set(k,x)}const v=n(d.metric?.usPerTick);x.count++;x.total+=v;x.max=Math.max(x.max,v);x.nanos+=n(d.metric?.totalNanos);x.calls+=n(d.metric?.calls)}for(const x of map.values())x.avgCall=x.calls>0?x.nanos/1000/x.calls:0;return [...map.values()].sort((a,b)=>b.total-a.total)}
function saveUiState(){try{localStorage.setItem(UI_STORAGE_KEY,JSON.stringify({mode:siteMode,view:currentView,gridState,deviceState,compareState}))}catch(e){}}
function restoreUiState(){try{const s=JSON.parse(localStorage.getItem(UI_STORAGE_KEY)||'null');if(!s)return;if(s.gridState)Object.assign(gridState,s.gridState);if(s.deviceState)Object.assign(deviceState,s.deviceState);if(s.compareState)Object.assign(compareState,s.compareState);if(['overview','grids','devices','compare'].includes(s.view))currentView=s.view;if(['simple','expert'].includes(s.mode))siteMode=s.mode}catch(e){}}
function setSelectValue(id,value,fallback='*'){const el=document.getElementById(id);if([...el.options].some(o=>o.value===String(value)))el.value=String(value);else el.value=fallback}
function syncStateToControls(){document.getElementById('gridSearch').value=gridState.q||'';setSelectValue('gridDimension',gridState.dimension);document.getElementById('gridMin').value=gridState.min;setSelectValue('gridSort',gridState.sort,'core');setSelectValue('gridTop',gridState.top,'50');document.getElementById('gridActive').checked=!!gridState.active;document.getElementById('deviceSearch').value=deviceState.q||'';setSelectValue('deviceDimension',deviceState.dimension);document.getElementById('deviceMin').value=deviceState.min;setSelectValue('deviceSort',deviceState.sort,'time');setSelectValue('deviceTop',deviceState.top,'50');setSelectValue('compareKind',compareState.kind,'all');setSelectValue('compareSort',compareState.sort,'delta');document.getElementById('comparePct').value=compareState.minPct;document.getElementById('compareAbs').value=compareState.minAbs;setSelectValue('compareTop',compareState.top,'100')}
function dimensionStats(){const map=new Map();for(const g of allGrids){const d=g.dimension||'unknown';let x=map.get(d);if(!x){x={dimension:d,count:0,core:0,max:0,devices:0,scheduler:0};map.set(d,x)}const c=valueOf(g,'core');x.count++;x.core+=c;x.max=Math.max(x.max,c);x.devices+=valueOf(g,'devices');x.scheduler+=valueOf(g,'scheduler')}return [...map.values()].sort((a,b)=>b.core-a.core)}
function globalStats(){return{core:allGrids.reduce((a,g)=>a+valueOf(g,'core'),0),devices:allGrids.reduce((a,g)=>a+valueOf(g,'devices'),0),scheduler:allGrids.reduce((a,g)=>a+valueOf(g,'scheduler'),0),grids:allGrids.length,dims:new Set(allGrids.map(g=>g.dimension||'unknown')).size,linked:allDevices.filter(d=>d.gridLabel).length}}
function gridRows(grids,limit=0){const total=globalStats().core;const rows=(limit?grids.slice(0,limit):grids).map(g=>`<tr class="clickable" data-grid="${esc(g.label+'|'+g.dimension+'|'+anchorText(g))}"><td class="left">${badge(valueOf(g,'core'))} <b>${esc(g.label)}</b></td><td>${fmt(valueOf(g,'core'))}</td><td>${pctText(valueOf(g,'core'),total)}</td><td>${fmt(valueOf(g,'devices'))}</td><td>${fmt(valueOf(g,'scheduler'))}</td><td>${fmt(valueOf(g,'overhead'))}</td><td class="left dim" title="${esc(g.dimension)}">${esc(dimShort(g.dimension))}</td><td>${esc(anchorText(g))}</td><td>${coordActions(g.dimension,g.anchor)}</td></tr>`).join('');return `<div class="table-scroll compact"><table class="data-table"><thead><tr><th>Grid</th><th>Core</th><th>Share</th><th>Devices</th><th>Scheduler</th><th>Overhead</th><th class="left">Dimension</th><th>Anchor</th><th>Actions</th></tr></thead><tbody>${rows}</tbody></table></div>`}
function bindGridRows(root,grids){root.querySelectorAll('tbody tr').forEach((tr,i)=>tr.onclick=e=>{if(e.target.closest('[data-copy-text]'))return;selectedGrid=grids[i];switchView('grids')})}
function deviceRows(devices,limit=0){const arr=limit?devices.slice(0,limit):devices;const total=allDevices.reduce((a,d)=>a+n(d.metric?.usPerTick),0);const rows=arr.map(d=>`<tr><td class="left clip" title="${esc(d.type)}">${badge(d.metric?.usPerTick)} <b>${esc(d.type||'unknown')}</b></td><td>${fmt(d.metric?.usPerTick)}</td><td>${pctText(d.metric?.usPerTick,total)}</td><td>${fmtCall(avgCallUs(d.metric))}</td><td>${calls(d.metric?.callsPerTick)}</td><td>${gridLink(d.gridLabel)}</td><td class="left dim" title="${esc(d.dimension)}">${esc(dimShort(d.dimension))}</td><td>${esc(posText(d.position))}</td><td>${coordActions(d.dimension,d.position)}</td></tr>`).join('');return `<div class="table-scroll compact"><table class="data-table"><thead><tr><th>Physical device</th><th>Load</th><th>Share</th><th>Avg / call</th><th>Calls</th><th>Grid</th><th class="left">Dimension</th><th>Position</th><th>Actions</th></tr></thead><tbody>${rows}</tbody></table></div>`}
function simpleDeviceHtml(d){const load=n(d.metric?.usPerTick);return `<div class="simple-device"><div><b>${esc(humanType(d.type))}</b><div class="where">${esc(dimShort(d.dimension))} · ${esc(posText(d.position))} · ${d.gridLabel?esc(d.gridLabel):'unlinked'}</div></div><div><b>${fmt(load)}</b><div class="muted">${fmtCall(avgCallUs(d.metric))}</div></div><div>${esc(deviceFrequencyText(d))}</div><div class="simple-actions">${coordActions(d.dimension,d.position)}${d.gridLabel?`<button data-open-grid="${esc(d.gridLabel)}">Открыть сеть</button>`:''}</div></div>`}
function renderSimple(){const s=globalStats(),physicalTotal=allDevices.reduce((a,d)=>a+n(d.metric?.usPerTick),0),topGrid=[...allGrids].sort((a,b)=>valueOf(b,'core')-valueOf(a,'core'))[0]||null;const [grade,gradeClass]=simpleGrade(s.core),budget=tickBudgetPct(s.core);document.getElementById('simpleHero').innerHTML=`<h2>Что сейчас грузит AE2</h2><div class="big ${gradeClass}">${ms(s.core)} · ${budget.toFixed(budget>=10?1:2)}% от 50 ms/t</div><div class="lead"><b>${grade}.</b> Это сумма измеренного Grid Core по сетям AE2, а не полный MSPT сервера.${topGrid?` Самая тяжёлая сеть сейчас — <b>${esc(topGrid.label)}</b> в <b>${esc(dimShort(topGrid.dimension))}</b>: ${fmt(valueOf(topGrid,'core'))}.`:''}</div>`;document.getElementById('simpleCards').innerHTML=`<div class="card"><small>Всего сетей</small><b>${s.grids}</b></div><div class="card"><small>Dimensions</small><b>${s.dims}</b></div><div class="card"><small>Работа устройств внутри Grid</small><b>${ms(s.devices)}</b></div><div class="card"><small>Физические targets</small><b>${ms(physicalTotal)}</b></div><div class="card"><small>Привязано к Grid</small><b>${s.linked}/${allDevices.length}</b></div>`;
 const grids=[...allGrids].sort((a,b)=>valueOf(b,'core')-valueOf(a,'core')).slice(0,6);document.getElementById('simpleProblems').innerHTML=grids.map((g,i)=>`<div class="simple-issue ${i===0?'hot':''}"><h3>${i===0?'Главная проблема':'Следующая по нагрузке'} · ${esc(g.label)}</h3><div class="simple-load">${fmt(valueOf(g,'core'))}</div><div class="where muted">${esc(dimShort(g.dimension))} · anchor ${esc(anchorText(g))}</div><div class="simple-why">${esc(simpleGridCause(g))}</div><div class="simple-actions">${coordActions(g.dimension,g.anchor)}<button data-open-grid="${esc(g.label)}">Подробнее</button></div></div>`).join('')||'<div class="empty">Нет Grid данных.</div>';
 const dims=dimensionStats().slice(0,8);document.getElementById('simpleDimensions').innerHTML=`<div class="table-scroll compact"><table class="data-table"><thead><tr><th>Где</th><th>Сетей</th><th>AE2 нагрузка</th><th>Самая тяжёлая сеть</th></tr></thead><tbody>${dims.map(x=>`<tr><td class="left">${esc(dimShort(x.dimension))}</td><td>${x.count}</td><td>${fmt(x.core)}</td><td>${fmt(x.max)}</td></tr>`).join('')}</tbody></table></div>`;
 const devices=[...allDevices].filter(d=>n(d.metric?.usPerTick)>0).sort((a,b)=>n(b.metric?.usPerTick)-n(a.metric?.usPerTick)).slice(0,10);document.getElementById('simpleDevices').innerHTML=devices.map(simpleDeviceHtml).join('')||'<div class="empty">Нет активных физических AE2-устройств.</div>';renderSimpleCompare()}
function renderOverview(){const s=globalStats();const physicalTotal=allDevices.reduce((a,d)=>a+n(d.metric?.usPerTick),0);document.getElementById('globalCards').innerHTML=`<div class="card"><small>Σ Grid Core</small><b>${fmt(s.core)}</b></div><div class="card"><small>Σ Grid Devices</small><b>${fmt(s.devices)}</b></div><div class="card"><small>Σ Physical targets</small><b>${fmt(physicalTotal)}</b></div><div class="card"><small>AE2 Grids</small><b>${s.grids.toLocaleString()}</b></div><div class="card"><small>Linked physical</small><b>${s.linked}/${allDevices.length}</b></div>`;
 const top=[...allGrids].sort((a,b)=>valueOf(b,'core')-valueOf(a,'core')).slice(0,20);const tg=document.getElementById('topGrids');tg.innerHTML=gridRows(top);bindGridRows(tg,top);
 const dims=dimensionStats().slice(0,15);document.getElementById('dimensionSummary').innerHTML=`<div class="table-scroll compact"><table class="data-table"><thead><tr><th>Dimension</th><th>Grids</th><th>Σ Core</th><th>Share</th><th>Max Core</th><th>Σ Devices</th></tr></thead><tbody>${dims.map(x=>`<tr class="clickable"><td class="left dim" title="${esc(x.dimension)}">${esc(dimShort(x.dimension))}</td><td>${x.count}</td><td>${fmt(x.core)}</td><td>${pctText(x.core,s.core)}</td><td>${fmt(x.max)}</td><td>${fmt(x.devices)}</td></tr>`).join('')}</tbody></table></div>`;document.querySelectorAll('#dimensionSummary tbody tr').forEach((tr,i)=>tr.onclick=()=>{document.getElementById('gridDimension').value=dims[i].dimension;switchView('grids')});
 const devices=[...allDevices].sort((a,b)=>n(b.metric?.usPerTick)-n(a.metric?.usPerTick));document.getElementById('topDevices').innerHTML=devices.length?deviceRows(devices,20):'<div class="empty">В этом отчёте нет physicalDevices.</div>';
 const pg=physicalByGridStats().slice(0,20);document.getElementById('physicalByGrid').innerHTML=pg.length?`<div class="table-scroll compact"><table class="data-table"><thead><tr><th>Grid</th><th>Targets</th><th>Σ physical</th><th>Share</th><th>Max target</th></tr></thead><tbody>${pg.map(x=>`<tr class="${x.gridLabel==='(unlinked)'?'':'clickable'}" data-open-grid="${x.gridLabel==='(unlinked)'?'':esc(x.gridLabel)}"><td class="left">${x.gridLabel==='(unlinked)'?'<span class="muted">unlinked</span>':esc(x.gridLabel)}</td><td>${x.count}</td><td>${fmt(x.total)}</td><td>${pctText(x.total,physicalTotal)}</td><td>${fmt(x.max)}</td></tr>`).join('')}</tbody></table></div>`:'<div class="empty">Нет physical devices</div>';
 const types=deviceTypeStats().slice(0,20);document.getElementById('hotspotTypes').innerHTML=types.length?`<div class="table-scroll compact"><table class="data-table"><thead><tr><th>Type</th><th>Positions</th><th>Σ load</th><th>Share</th><th>Avg/call</th></tr></thead><tbody>${types.map(x=>`<tr><td class="left clip" title="${esc(x.type)}">${esc(x.type)}</td><td>${x.count}</td><td>${fmt(x.total)}</td><td>${pctText(x.total,physicalTotal)}</td><td>${fmtCall(x.avgCall)}</td></tr>`).join('')}</tbody></table></div>`:'<div class="empty">Нет данных</div>';
}
function applyGridFilters(sync=true){if(sync){gridState.q=document.getElementById('gridSearch').value.trim().toLowerCase();gridState.dimension=document.getElementById('gridDimension').value;gridState.min=Math.max(0,n(document.getElementById('gridMin').value));gridState.sort=document.getElementById('gridSort').value;gridState.top=parseInt(document.getElementById('gridTop').value||'0',10)||0;gridState.active=document.getElementById('gridActive').checked}
 let arr=allGrids.filter(g=>{if(gridState.dimension!=='*'&&(g.dimension||'unknown')!==gridState.dimension)return false;if(valueOf(g,'core')<gridState.min)return false;if(gridState.active&&valueOf(g,'devices')<=0)return false;if(gridState.q){const hay=`${g.label} ${g.dimension} ${anchorText(g)}`.toLowerCase();if(!hay.includes(gridState.q))return false}return true});const matched=arr.length;arr.sort((a,b)=>valueOf(b,gridState.sort)-valueOf(a,gridState.sort)||valueOf(b,'core')-valueOf(a,'core'));if(gridState.top>0)arr=arr.slice(0,gridState.top);gridFiltered=arr;if(!selectedGrid||!arr.includes(selectedGrid))selectedGrid=arr[0]||null;renderGridStatus(matched);renderGridList();renderSelection();renderGridDetail();saveUiState()}
function renderGridStatus(matched){document.getElementById('gridStatus').innerHTML=`<span>Показано <strong>${gridFiltered.length}</strong> из <strong>${allGrids.length}</strong></span><span>Фильтру соответствуют: ${matched}</span><span>Core ≥ ${gridState.min} µs/t</span>`}
function renderGridList(){const root=document.getElementById('gridList');if(!gridFiltered.length){root.innerHTML='<div class="panel empty">По текущему фильтру сетей нет.</div>';return}root.innerHTML=gridRows(gridFiltered);root.querySelectorAll('tbody tr').forEach((tr,i)=>{if(gridFiltered[i]===selectedGrid)tr.classList.add('selected');tr.onclick=e=>{if(e.target.closest('[data-copy-text]'))return;selectedGrid=gridFiltered[i];renderGridList();renderSelection();renderGridDetail()}})}
function renderSelection(){const g=selectedGrid,root=document.getElementById('selectionSummary');if(!g){root.innerHTML='<div class="empty">Сеть не выбрана</div>';return}root.innerHTML=`<div class="muted">${esc(g.label)} · ${esc(dimShort(g.dimension))}</div><div class="big">${fmt(valueOf(g,'core'))}</div><div class="kv"><span>Devices</span><b>${fmt(valueOf(g,'devices'))}</b><span>Scheduler</span><b>${fmt(valueOf(g,'scheduler'))}</b><span>Grid overhead</span><b>${fmt(valueOf(g,'overhead'))}</b><span>Services</span><b>${fmt(valueOf(g,'services'))}</b><span>Anchor</span><b>${esc(anchorText(g))}</b></div><div style="margin-top:10px">${coordActions(g.dimension,g.anchor)}</div>`}
function renderGridDetail(){const g=selectedGrid,root=document.getElementById('gridDetail');if(!g){root.innerHTML='';return}const m=g.metrics||{},tm=g.tickManager,core=Math.max(1,n(m.gridCore?.usPerTick));let html=`<div class="meta coord-line"><span>${esc(g.dimension)}</span><span>anchor: ${esc(anchorText(g))}</span>${coordActions(g.dimension,g.anchor)}</div><div class="cards"><div class="card"><small>Grid Core</small><b>${fmt(m.gridCore?.usPerTick)}</b></div><div class="card"><small>Devices</small><b>${fmt(m.devices?.usPerTick)}</b></div><div class="card"><small>Scheduler</small><b>${fmt(tm?.schedulerRemainder?.usPerTick)}</b></div><div class="card"><small>Grid overhead</small><b>${fmt(m.gridOverhead?.usPerTick)}</b></div><div class="card"><small>Services</small><b>${fmt(m.gridServices?.usPerTick)}</b></div></div>`;
 html+=section('Overview',metric('Grid Core (inclusive)',m.gridCore,core,0,true)+metric('Grid Services (sum)',m.gridServices,core,1)+metric('Grid overhead (exclusive)',m.gridOverhead,core,1)+metric('Devices',m.devices,core,1));const services=(g.services||[]).slice().sort((a,b)=>n(b.total?.usPerTick)-n(a.total?.usPerTick));html+=section('Services',services.map(s=>metric(`${s.name} · ${s.className}`,s.total,Math.max(1,n(m.gridServices?.usPerTick)))).join(''));const gridDevices=allDevices.filter(d=>d.gridLabel===g.label).sort((a,b)=>n(b.metric?.usPerTick)-n(a.metric?.usPerTick));html+=section(`Top physical devices in ${g.label}`,gridDevices.length?deviceRows(gridDevices,25):'<div class="empty">Для этой Grid физические timing targets пока не связаны. Пассивные или неактивные устройства могут остаться unlinked.</div>');
 let adv='';const cp=g.corePhases||{};adv+=section('Grid Core phases',metric('Server start',cp.server_start,core)+metric('Level start',cp.level_start,core)+metric('Level end',cp.level_end,core)+metric('Server end',cp.server_end,core));if(tm){const max=Math.max(1,n(tm.service?.usPerTick));adv+=section('Tick Manager',metric('Service (inclusive)',tm.service,max,0,true)+metric('Level queue',tm.levelQueue,max,1)+metric('Queue (inclusive)',tm.queue,max,1,true)+metric('Devices',tm.devices,max,2)+metric('Scheduler remainder',tm.schedulerRemainder,max,2,true)+metric('Queue residual + guard',tm.queueResidual,max,2)+metric('Level dispatch overhead',tm.levelDispatchOverhead,max,1)+metric('Outer overhead',tm.outerOverhead,max,1));const dl=(tm.dispatchLevels||[]).slice().sort((a,b)=>n(b.total?.callsPerTick)-n(a.total?.callsPerTick)||n(b.total?.usPerTick)-n(a.total?.usPerTick));if(dl.length){const same=dl.filter(x=>x.anchorDimension),foreign=dl.filter(x=>!x.anchorDimension);const sum=(arr,key)=>arr.reduce((a,x)=>a+n(x.total?.[key]),0);adv+=`<section><h2>Actual Level dispatch</h2><div class="meta"><span>Levels: <b>${dl.length}</b></span><span class="anchor">Anchor: ${same.length} · ${fmt(sum(same,'usPerTick'))}</span><span class="foreign">Foreign: ${foreign.length} · ${fmt(sum(foreign,'usPerTick'))}</span></div><div class="dispatch-scroll"><table class="data-table"><thead><tr><th>Actual Level</th><th>Relation</th><th>Total</th><th>Calls</th><th>LevelEnd</th></tr></thead><tbody>${dl.map(x=>`<tr><td class="left dim" title="${esc(x.dimension)}">${esc(dimShort(x.dimension))}</td><td class="${x.anchorDimension?'anchor':'foreign'}">${x.anchorDimension?'anchor':'foreign'}</td><td>${fmt(x.total?.usPerTick)}</td><td>${calls(x.total?.callsPerTick)}</td><td>${fmt(x.levelEnd?.usPerTick)}</td></tr>`).join('')}</tbody></table></div></section>`}const q=tm.queuePhases||{},qmax=Math.max(1,n(tm.queue?.usPerTick));adv+=section('Tick Queue phases',metric('Head due-check',q.headDueCheck,qmax)+metric('Poll + dequeue prep',q.dequeuePrep,qmax)+metric('Tick-rate update',q.rateUpdate,qmax)+metric('Awake-map check',q.awakeCheck,qmax)+metric('PriorityQueue reinsert',q.reinsert,qmax)+metric('Sleep branch',q.sleepBranch,qmax)+metric('Future-head stop',q.futureStop,qmax));const c=tm.controls||{};adv+=section('Controls (nested)',metric('Sleep',c.sleep,max)+metric('Wake',c.wake,max)+metric('Alert',c.alert,max));const mods=tm.modulation||{};adv+=`<section><h2>Tick modulation</h2><div class="mods">${Object.entries(mods).map(([k,v])=>`<div class="mod"><span class="muted">${esc(k)}</span><strong>${calls(v.perTick)}</strong><span class="muted">${n(v.count).toLocaleString()} calls</span></div>`).join('')}</div></section>`}html+=`<details class="advanced"><summary>Advanced diagnostics · Tick Manager / queue / Level dispatch</summary><div class="advanced-body">${adv}</div></details>`;root.innerHTML=html}
function applyDeviceFilters(sync=true){if(sync){deviceState.q=document.getElementById('deviceSearch').value.trim().toLowerCase();deviceState.dimension=document.getElementById('deviceDimension').value;deviceState.min=Math.max(0,n(document.getElementById('deviceMin').value));deviceState.sort=document.getElementById('deviceSort').value;deviceState.top=parseInt(document.getElementById('deviceTop').value||'0',10)||0}let arr=allDevices.filter(d=>{if(deviceState.dimension!=='*'&&(d.dimension||'unknown')!==deviceState.dimension)return false;if(n(d.metric?.usPerTick)<deviceState.min)return false;if(deviceState.q){const hay=`${d.type} ${d.gridLabel||''} ${d.dimension} ${posText(d.position)}`.toLowerCase();if(!hay.includes(deviceState.q))return false}return true});const matched=arr.length;arr.sort((a,b)=>deviceState.sort==='avg'?avgCallUs(b.metric)-avgCallUs(a.metric):deviceState.sort==='calls'?n(b.metric?.callsPerTick)-n(a.metric?.callsPerTick):deviceState.sort==='type'?String(a.type).localeCompare(String(b.type)):n(b.metric?.usPerTick)-n(a.metric?.usPerTick));if(deviceState.top>0)arr=arr.slice(0,deviceState.top);deviceFiltered=arr;document.getElementById('deviceStatus').innerHTML=`<span>Показано <strong>${arr.length}</strong> из <strong>${allDevices.length}</strong> physical devices</span><span>Фильтру соответствуют: ${matched}</span><span>Load ≥ ${deviceState.min} µs/t</span><span>Linked: ${arr.filter(d=>d.gridLabel).length}</span>`;renderTypeSummary();document.getElementById('deviceList').innerHTML=arr.length?deviceRows(arr):'<div class="panel empty">Нет physical devices по текущему фильтру.</div>';saveUiState()}
function renderTypeSummary(){const map=new Map();for(const d of allDevices){const k=d.type||'unknown';let x=map.get(k);if(!x){x={type:k,count:0,total:0,max:0,calls:0};map.set(k,x)}const v=n(d.metric?.usPerTick);x.count++;x.total+=v;x.max=Math.max(x.max,v);x.calls+=n(d.metric?.callsPerTick)}const top=[...map.values()].sort((a,b)=>b.total-a.total).slice(0,15);document.getElementById('typeSummary').innerHTML=top.length?`<div class="type-grid">${top.map(x=>`<div class="type-card"><small>${esc(x.type)}</small><strong>${fmt(x.total)}</strong><small>${x.count} positions · max ${fmt(x.max)} · ${calls(x.calls)}</small></div>`).join('')}</div>`:''}
const reportTotal=(r,kind)=>kind==='physical'?[...(r?.physicalDevices||[])].reduce((a,d)=>a+n(d.metric?.usPerTick),0):[...(r?.grids||[])].reduce((a,g)=>a+n(g.metrics?.gridCore?.usPerTick),0);
function gridDeviceSets(reportObj){const map=new Map();for(const d of (reportObj?.physicalDevices||[])){if(!d.gridLabel)continue;let set=map.get(d.gridLabel);if(!set){set=new Set();map.set(d.gridLabel,set)}set.add(deviceKey(d))}return map}
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
function gridCompareRows(rows){if(!rows.length)return '<div class="empty">Нет Grid, подходящих под фильтр.</div>';const body=rows.map(x=>{const g=x.cur||x.base,pos=g?.anchor,dim=g?.dimension||'unknown',labels=x.cur&&x.base&&x.cur.label!==x.base.label?`${esc(x.base.label)} → ${esc(x.cur.label)}`:esc(g?.label||'');return `<tr><td class="left"><span class="status-pill ${deltaClass(x)}">${x.status}</span> ${x.cur?`<button class="grid-link" data-open-grid="${esc(x.cur.label)}">${labels}</button>`:labels}<div class="muted">${esc(gridMatchText(x))}</div></td><td>${fmt(x.cv)}</td><td>${fmt(x.bv)}</td><td class="${deltaClass(x)}">${deltaText(x.delta)}</td><td class="${deltaClass(x)}">${pctChange(x)}</td><td class="left dim" title="${esc(dim)}">${esc(dimShort(dim))}</td><td>${esc(posText(pos))}</td><td>${coordActions(dim,pos)}</td></tr>`}).join('');return `<div class="table-scroll"><table class="data-table"><thead><tr><th>Grid</th><th>Current</th><th>Baseline</th><th>Δ Core</th><th>Change</th><th>Dimension</th><th>Anchor</th><th>Actions</th></tr></thead><tbody>${body}</tbody></table></div>`}
function deviceCompareRows(rows){if(!rows.length)return '<div class="empty">Нет устройств, подходящих под фильтр.</div>';const body=rows.map(x=>{const d=x.cur||x.base,pos=d?.position,dim=d?.dimension||'unknown';return `<tr><td class="left clip"><span class="status-pill ${deltaClass(x)}">${x.status}</span> <b>${esc(d?.type||'unknown')}</b></td><td>${fmt(x.cv)}</td><td>${fmt(x.bv)}</td><td class="${deltaClass(x)}">${deltaText(x.delta)}</td><td class="${deltaClass(x)}">${pctChange(x)}</td><td>${x.cur?gridLink(x.cur.gridLabel):'<span class="muted">—</span>'}</td><td class="left dim" title="${esc(dim)}">${esc(dimShort(dim))}</td><td>${esc(posText(pos))}</td><td>${coordActions(dim,pos)}</td></tr>`}).join('');return `<div class="table-scroll"><table class="data-table"><thead><tr><th>Device</th><th>Current</th><th>Baseline</th><th>Δ Load</th><th>Change</th><th>Grid</th><th>Dimension</th><th>Position</th><th>Actions</th></tr></thead><tbody>${body}</tbody></table></div>`}
function renderCompare(){if(!baselineReport){document.getElementById('compareMeta').innerHTML='<span>Baseline не выбран.</span>';document.getElementById('compareCards').innerHTML='';document.getElementById('compareGridList').innerHTML='<div class="empty">Выбери baseline JSON.</div>';document.getElementById('compareDeviceList').innerHTML='<div class="empty">Выбери baseline JSON.</div>';return}const gd=gridDiffRows(report,baselineReport),dd=diffRows(allDevices,baselineReport.physicalDevices||[],deviceKey,d=>n(d.metric?.usPerTick));const gf=filterDiff(gd),df=filterDiff(dd);const cg=reportTotal(report,'grid'),bg=reportTotal(baselineReport,'grid'),cp=reportTotal(report,'physical'),bp=reportTotal(baselineReport,'physical');const gdlt=cg-bg,pdlt=cp-bp,fingerprint=gd.filter(x=>x.matchKind==='devices').length;document.getElementById('compareMeta').innerHTML=`<span>Current: <b>${esc(report.generatedAt||'this report')}</b></span><span>Baseline: <b>${esc(baselineName||baselineReport.generatedAt||'selected JSON')}</b></span><span>Grid match: unique anchor first; changed/duplicate anchors → unambiguous ≥80% physical-device fingerprint</span><span>Duplicate anchor groups: <b>${duplicateAnchorGroups(allGrids)}</b> current / <b>${duplicateAnchorGroups(baselineReport.grids||[])}</b> baseline</span><span>Fingerprint fallback matched: <b>${fingerprint}</b></span><span>Devices: dimension + type + position</span>`;document.getElementById('compareCards').innerHTML=`<div class="card"><small>Grid Core Current</small><b>${fmt(cg)}</b></div><div class="card"><small>Grid Core Δ</small><b class="${gdlt>0?'delta-up':gdlt<0?'delta-down':''}">${deltaText(gdlt)}</b></div><div class="card"><small>Physical Current</small><b>${fmt(cp)}</b></div><div class="card"><small>Physical Δ</small><b class="${pdlt>0?'delta-up':pdlt<0?'delta-down':''}">${deltaText(pdlt)}</b></div><div class="card"><small>New / Gone</small><b>${gd.filter(x=>x.status==='new').length+dd.filter(x=>x.status==='new').length} / ${gd.filter(x=>x.status==='gone').length+dd.filter(x=>x.status==='gone').length}</b></div>`;document.getElementById('compareGridList').innerHTML=gridCompareRows(gf);document.getElementById('compareDeviceList').innerHTML=deviceCompareRows(df)}
function renderSimpleCompare(){const meta=document.getElementById('simpleCompareMeta'),box=document.getElementById('simpleCompare');if(!baselineReport){meta.innerHTML='<span>Прошлый профиль не выбран.</span>';box.innerHTML='';return}const gd=gridDiffRows(report,baselineReport),dd=diffRows(allDevices,baselineReport.physicalDevices||[],deviceKey,d=>n(d.metric?.usPerTick)),cg=reportTotal(report,'grid'),bg=reportTotal(baselineReport,'grid'),delta=cg-bg,pct=bg>0?delta/bg*100:0;meta.innerHTML=`<span>Сравниваем с: <b>${esc(baselineName||baselineReport.generatedAt||'baseline')}</b></span><span>Grid identity fallback: <b>${gd.filter(x=>x.matchKind==='devices').length}</b></span><span>Повторяющиеся anchors: <b>${duplicateAnchorGroups(allGrids)}</b> / <b>${duplicateAnchorGroups(baselineReport.grids||[])}</b></span>`;const worseG=gd.filter(x=>x.status==='regression').sort((a,b)=>b.delta-a.delta).slice(0,5),betterG=gd.filter(x=>x.status==='improvement').sort((a,b)=>a.delta-b.delta).slice(0,3),worseD=dd.filter(x=>x.status==='regression').sort((a,b)=>b.delta-a.delta).slice(0,5);const summary=delta>0?`AE2 в этом профиле тяжелее на ${fmt(Math.abs(delta))} (${pct.toFixed(1)}%).`:delta<0?`AE2 в этом профиле легче на ${fmt(Math.abs(delta))} (${Math.abs(pct).toFixed(1)}%).`:'Суммарная AE2-нагрузка почти не изменилась.';const gridItems=worseG.map(x=>{const g=x.cur||x.base;return `<div class="simple-issue"><h3>Сеть стала тяжелее · ${esc(g?.label||'')}</h3><div class="simple-load delta-up">+${fmt(x.delta)}</div><div class="simple-why">Было ${fmt(x.bv)}, стало ${fmt(x.cv)}. ${esc(gridMatchText(x))}</div><div class="simple-actions">${x.cur?coordActions(x.cur.dimension,x.cur.anchor):''}${x.cur?`<button data-open-grid="${esc(x.cur.label)}">Подробнее</button>`:''}</div></div>`}).join('');const deviceItems=worseD.map(x=>{const d=x.cur||x.base;return `<div class="simple-device"><div><b>${esc(humanType(d?.type))}</b><div class="where">${esc(dimShort(d?.dimension))} · ${esc(posText(d?.position))}</div></div><div><b class="delta-up">+${fmt(x.delta)}</b><div class="muted">${fmt(x.bv)} → ${fmt(x.cv)}</div></div><div>${x.cur?esc(deviceFrequencyText(x.cur)):''}</div><div class="simple-actions">${coordActions(d?.dimension,d?.position)}${x.cur?.gridLabel?`<button data-open-grid="${esc(x.cur.gridLabel)}">Открыть сеть</button>`:''}</div></div>`}).join('');const improved=betterG.length?`<div class="muted" style="margin-top:10px">Стало легче: ${betterG.map(x=>`${esc((x.cur||x.base)?.label)} ${deltaText(x.delta)}`).join(' · ')}</div>`:'';box.innerHTML=`<div class="simple-compare-summary ${delta>0?'delta-up':delta<0?'delta-down':''}"><b>${summary}</b></div>${gridItems?`<h3>Что сильнее всего ухудшилось</h3><div class="simple-issues">${gridItems}</div>`:'<div class="plain-good">Сильных регрессий Grid не найдено.</div>'}${deviceItems?`<h3 style="margin-top:14px">Какие устройства стали тяжелее</h3><div class="simple-list">${deviceItems}</div>`:''}${improved}`}
function syncCompareState(){compareState.kind=document.getElementById('compareKind').value;compareState.sort=document.getElementById('compareSort').value;compareState.minPct=Math.max(0,n(document.getElementById('comparePct').value));compareState.minAbs=Math.max(0,n(document.getElementById('compareAbs').value));compareState.top=parseInt(document.getElementById('compareTop').value||'0',10)||0;saveUiState();renderCompare()}
async function loadBaselineFile(file,openExpert=false){if(!file)return;try{const text=await file.text();const parsed=JSON.parse(text);if(!parsed||!Array.isArray(parsed.grids))throw new Error('JSON does not contain grids[]');baselineReport=parsed;baselineName=file.name||'';renderSimpleCompare();if(openExpert){setSiteMode('expert',false);switchView('compare')}else renderSimple()}catch(e){baselineReport=null;baselineName='';document.getElementById('compareMeta').innerHTML=`<span class="delta-up">Не удалось открыть baseline: ${esc(e.message||e)}</span>`;document.getElementById('simpleCompareMeta').innerHTML=`<span class="delta-up">Не удалось открыть baseline: ${esc(e.message||e)}</span>`;renderCompare();renderSimpleCompare()}}
function copySummary(){const s=globalStats();if(currentView==='grids'&&selectedGrid){const g=selectedGrid;return `${g.label}: Core ${fmt(valueOf(g,'core'))}, Devices ${fmt(valueOf(g,'devices'))}, Scheduler ${fmt(valueOf(g,'scheduler'))}, Overhead ${fmt(valueOf(g,'overhead'))}, ${g.dimension} @ ${anchorText(g)}`}return `AE2 Overview: ${allGrids.length} grids, Σ Core ${fmt(s.core)}, Σ Devices ${fmt(s.devices)}, Σ Scheduler ${fmt(s.scheduler)}, ${s.dims} dimensions, ${allDevices.length} physical devices`}

const gridDims=[...new Set(allGrids.map(g=>g.dimension||'unknown'))].sort((a,b)=>a.localeCompare(b));const deviceDims=[...new Set(allDevices.map(d=>d.dimension||'unknown'))].sort((a,b)=>a.localeCompare(b));populateDimensionSelect('gridDimension',gridDims);populateDimensionSelect('deviceDimension',deviceDims);
restoreUiState();syncStateToControls();
document.getElementById('reportMeta').innerHTML=`<span>Профиль: ${n(report.profileTicks).toLocaleString()} ticks</span><span>Grids: ${allGrids.length}</span><span>Physical devices: ${allDevices.length}</span><span>Linked: ${allDevices.filter(d=>d.gridLabel).length}</span><span>Dimensions: ${new Set(allGrids.map(g=>g.dimension||'unknown')).size}</span><span>${esc(report.generatedAt||'')}</span>`;
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

        for (GridSnapshot snapshot : snapshotsSorted()) {
            if (snapshot.gridCoreNanos <= 0L || snapshot.gridCoreCalls <= 0
                    || snapshot.anchorEntity == null || snapshot.virtualBase == null
                    || !(snapshot.anchorEntity.getLevel() instanceof ServerLevel)) {
                continue;
            }
            publishMetric(profiler, snapshot.anchorEntity.getLevel(), snapshot.virtualBase, 0,
                    snapshot.label + " / Total (inclusive)", snapshot.gridCoreNanos, snapshot.gridCoreCalls);
        }
    }

    private static String buildDetailedReportJson(List<GridSnapshot> snapshots, List<PhysicalDeviceSnapshot> physicalDevices, int profileTicks) {
        StringBuilder out = new StringBuilder(Math.max(4096, snapshots.size() * 4096));
        out.append("{\n");
        jsonField(out, 1, "format", "observable-ae2-grid-v18", true);
        jsonField(out, 1, "generatedAt", Instant.now().toString(), true);
        jsonNumberField(out, 1, "profileTicks", profileTicks, true);
        jsonField(out, 1, "note",
                "Inclusive/nested metrics must not be summed. Grid Core contains services; Tick Manager contains queue/devices. v20.1 is monitoring-only and adds dual simple/expert views plus duplicate-anchor-safe Compare identity: unique anchors match directly, while changed/duplicate anchors use an unambiguous physical-device fingerprint fallback; physical device links and operator navigation metadata remain available.", true);
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
            jsonMetricField(out, 3, "metric", device.nanos, device.calls, profileTicks, false);
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
            String dimension = null;
            if (snapshot.anchorEntity != null && snapshot.anchorEntity.getLevel() != null) {
                dimension = snapshot.anchorEntity.getLevel().dimension().location().toString();
            }
            jsonField(out, 3, "dimension", dimension == null ? "unknown" : dimension, true);

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

        indent(out, indentLevel + 1).append("\"dispatchLevels\": [\n");
        for (int i = 0; i < tickManager.dispatchLevels.size(); i++) {
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
            if (i + 1 < tickManager.dispatchLevels.size()) {
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

        List<GridSnapshot> snapshots = snapshotsSorted();
        Profiler profiler = Observable.INSTANCE.getPROFILER();

        for (GridSnapshot snapshot : snapshots) {
            if (snapshot.anchorEntity == null || !(snapshot.anchorEntity.getLevel() instanceof ServerLevel)
                    || snapshot.virtualBase == null) {
                continue;
            }

            Level level = snapshot.anchorEntity.getLevel();
            int slot = 0;

            slot = publishMetric(profiler, level, snapshot.virtualBase, slot,
                    snapshot.label + " / Grid Core (inclusive)",
                    snapshot.gridCoreNanos, snapshot.gridCoreCalls);

            slot = publishMetric(profiler, level, snapshot.virtualBase, slot,
                    snapshot.label + " / Grid Services (sum)",
                    snapshot.serviceTotalNanos, snapshot.serviceTotalCalls);

            slot = publishMetric(profiler, level, snapshot.virtualBase, slot,
                    snapshot.label + " / Grid overhead (exclusive)",
                    snapshot.gridOverheadNanos, snapshot.gridCoreCalls);

            slot = publishMetric(profiler, level, snapshot.virtualBase, slot,
                    snapshot.label + " / Devices (inside Tick Manager)",
                    snapshot.deviceNanos, snapshot.deviceCalls);

            TickManagerSnapshot tickManager = snapshot.tickManager;
            if (tickManager != null) {
                slot = publishMetric(profiler, level, snapshot.virtualBase, slot,
                        snapshot.label + " / TickMgr Queue (inclusive)",
                        tickManager.queue.nanos, tickManager.queue.calls);
                slot = publishMetric(profiler, level, snapshot.virtualBase, slot,
                        snapshot.label + " / TickMgr Scheduler remainder (queue - devices)",
                        tickManager.queueBookkeepingNanos, tickManager.queue.calls);
                slot = publishMetric(profiler, level, snapshot.virtualBase, slot,
                        snapshot.label + " / TickMgr Head due-check",
                        tickManager.headDueCheck.nanos, tickManager.headDueCheck.calls);
                slot = publishMetric(profiler, level, snapshot.virtualBase, slot,
                        snapshot.label + " / TickMgr Poll + dequeue prep",
                        tickManager.dequeuePrep.nanos, tickManager.dequeuePrep.calls);
                slot = publishMetric(profiler, level, snapshot.virtualBase, slot,
                        snapshot.label + " / TickMgr Tick-rate update",
                        tickManager.rateUpdate.nanos, tickManager.rateUpdate.calls);
                slot = publishMetric(profiler, level, snapshot.virtualBase, slot,
                        snapshot.label + " / TickMgr Awake-map check",
                        tickManager.awakeCheck.nanos, tickManager.awakeCheck.calls);
                slot = publishMetric(profiler, level, snapshot.virtualBase, slot,
                        snapshot.label + " / TickMgr PriorityQueue reinsert",
                        tickManager.reinsert.nanos, tickManager.reinsert.calls);
                slot = publishMetric(profiler, level, snapshot.virtualBase, slot,
                        snapshot.label + " / TickMgr Sleep branch",
                        tickManager.sleepBranch.nanos, tickManager.sleepBranch.calls);
                slot = publishMetric(profiler, level, snapshot.virtualBase, slot,
                        snapshot.label + " / TickMgr Future-head stop check",
                        tickManager.futureStop.nanos, tickManager.futureStop.calls);
                slot = publishMetric(profiler, level, snapshot.virtualBase, slot,
                        snapshot.label + " / TickMgr Queue residual + guard",
                        tickManager.queueResidualNanos, tickManager.queue.calls);
                slot = publishMetric(profiler, level, snapshot.virtualBase, slot,
                        snapshot.label + " / TickMgr Level dispatch overhead",
                        tickManager.levelDispatchNanos, tickManager.levelQueue.calls);
                slot = publishMetric(profiler, level, snapshot.virtualBase, slot,
                        snapshot.label + " / TickMgr Outer overhead",
                        tickManager.outerOverheadNanos, tickManager.serviceCalls);

                long controlNanos = tickManager.sleep.nanos + tickManager.wake.nanos + tickManager.alert.nanos;
                int controlCalls = tickManager.sleep.calls + tickManager.wake.calls + tickManager.alert.calls;
                slot = publishMetric(profiler, level, snapshot.virtualBase, slot,
                        snapshot.label + " / TickMgr Sleep-Wake-Alert (nested)",
                        controlNanos, controlCalls);
            }

            int remaining = MAX_VIRTUAL_MARKERS - slot;
            if (remaining <= 0) {
                continue;
            }

            int serviceLimit = Math.min(snapshot.services.size(), remaining);
            long omittedNanos = 0L;
            int omittedCalls = 0;

            // Leave one slot for an omitted aggregate if necessary.
            if (snapshot.services.size() > serviceLimit && serviceLimit > 0) {
                serviceLimit--;
            }

            for (int i = 0; i < snapshot.services.size(); i++) {
                ServiceSnapshot service = snapshot.services.get(i);
                if (service.total.nanos <= 0L || service.total.calls <= 0) {
                    continue;
                }
                if (i < serviceLimit) {
                    slot = publishMetric(profiler, level, snapshot.virtualBase, slot,
                            snapshot.label + " / Service " + service.displayName,
                            service.total.nanos, service.total.calls);
                } else {
                    omittedNanos += service.total.nanos;
                    omittedCalls += service.total.calls;
                }
            }

            if (omittedNanos > 0L && slot < MAX_VIRTUAL_MARKERS) {
                publishMetric(profiler, level, snapshot.virtualBase, slot,
                        snapshot.label + " / Other services (omitted from markers)",
                        omittedNanos, omittedCalls);
            }
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
        if (elapsed < 0L) {
            return;
        }
        metric.nanos += elapsed;
        metric.calls += 1;
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
                    score);
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
        private final BlockEntity host;
        private final String dimension;
        private final BlockPos position;
        private String preferredName;
        private String gridLabel;

        private PhysicalDeviceRef(BlockEntity host, String dimension, BlockPos position, String preferredName) {
            this.host = host;
            this.dimension = dimension;
            this.position = position;
            this.preferredName = preferredName;
        }
    }

    private static final class PhysicalDeviceSnapshot {
        private final String dimension;
        private final BlockPos position;
        private final String type;
        private final long nanos;
        private final int calls;
        private final String gridLabel;

        private PhysicalDeviceSnapshot(String dimension, BlockPos position, String type, long nanos, int calls,
                                       String gridLabel) {
            this.dimension = dimension;
            this.position = position;
            this.type = type;
            this.nanos = nanos;
            this.calls = calls;
            this.gridLabel = gridLabel;
        }
    }

    private static final class GridLifecycleToken {
        private final Object gridObject;
        private final GridInfo grid;
        private final Phase phase;
        private final long startedAt;
        private final long generation;

        private GridLifecycleToken(Object gridObject, GridInfo grid, Phase phase, long startedAt, long generation) {
            this.gridObject = gridObject;
            this.grid = grid;
            this.phase = phase;
            this.startedAt = startedAt;
            this.generation = generation;
        }
    }

    private static final class ServiceToken {
        private final GridInfo grid;
        private final ServiceInfo service;
        private final Phase phase;
        private final int dispatchLevelId;
        private final long startedAt;
        private final long generation;

        private ServiceToken(GridInfo grid, ServiceInfo service, Phase phase, int dispatchLevelId,
                             long startedAt, long generation) {
            this.grid = grid;
            this.service = service;
            this.phase = phase;
            this.dispatchLevelId = dispatchLevelId;
            this.startedAt = startedAt;
            this.generation = generation;
        }
    }

    private static final class TickManagerSectionToken {
        private final Object tickManager;
        private final GridInfo grid;
        private final TickSection section;
        private final long startedAt;
        private final long generation;

        private TickManagerSectionToken(Object tickManager, GridInfo grid, TickSection section,
                                        long startedAt, long generation) {
            this.tickManager = tickManager;
            this.grid = grid;
            this.section = section;
            this.startedAt = startedAt;
            this.generation = generation;
        }
    }

    private static final class TickQueueDetailToken {
        private final Object tickManager;
        private final GridInfo grid;
        private final long generation;
        private TickQueuePhase phase = TickQueuePhase.NONE;
        private long phaseStartedAt;

        private TickQueueDetailToken(Object tickManager, GridInfo grid, long generation) {
            this.tickManager = tickManager;
            this.grid = grid;
            this.generation = generation;
        }
    }

    private static final class TickManagerControlToken {
        private final Object tickManager;
        private final GridInfo grid;
        private final TickControl control;
        private final long startedAt;
        private final long generation;

        private TickManagerControlToken(Object tickManager, GridInfo grid, TickControl control,
                                        long startedAt, long generation) {
            this.tickManager = tickManager;
            this.grid = grid;
            this.control = control;
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

        private GridSnapshot(String label, BlockPos anchor, BlockEntity anchorEntity, BlockPos virtualBase,
                             long gridCoreNanos, int gridCoreCalls,
                             EnumMap<Phase, MetricSnapshot> gridPhases,
                             long serviceTotalNanos, int serviceTotalCalls,
                             long gridOverheadNanos,
                             long deviceNanos, int deviceCalls,
                             List<ServiceSnapshot> services, TickManagerSnapshot tickManager, long scoreNanos) {
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
        }
    }
}
