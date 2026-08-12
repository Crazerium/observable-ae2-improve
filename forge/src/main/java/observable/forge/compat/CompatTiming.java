package observable.forge.compat;

import net.minecraft.core.registries.BuiltInRegistries;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.world.item.Item;
import net.minecraft.world.level.Level;
import net.minecraft.world.level.block.entity.BlockEntity;
import observable.Observable;
import observable.Props;
import observable.server.Profiler;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;

/**
 * Optional profiler bridge for mods which tick machines outside vanilla's
 * Level#tickBlockEntities path (AE2 and the GT Odyssey GTCEu/GTO stack).
 *
 * This class intentionally has no compile-time dependency on AE2/GTCEu/GTO.
 */
public final class CompatTiming {
    private static final String[] OWNER_METHODS = {
            "getOwner",
            "getBlockEntity",
            "getHolder",
            "getMachine",
            "getHost",
            "getDrive"
    };

    private static final String[] OWNER_FIELDS = {
            "owner",
            "blockEntity",
            "blockEntityHolder",
            "holder",
            "machine",
            "metaMachine",
            "host",
            "drive",
            "chestOrDrive"
    };

    // DriveWatcher is a very hot AE2 storage path. Never walk an arbitrary
    // object graph from it: on large servers thousands of cell operations can
    // arrive in one tick, and recursive reflection here can turn profiling
    // overhead into a watchdog hang. These names cover AE2 generations and
    // forks; a type-based fallback is discovered once per runtime class.
    private static final String[] AE2_DRIVE_OWNER_FIELDS = {
            "chestOrDrive",
            "cord",
            "drive",
            "host",
            "owner",
            "blockEntity"
    };

    // Modern DriveWatcher variants can keep only a small activity/change callback
    // rather than a direct DriveBlockEntity field. We may inspect that one callback
    // and its direct captured receiver, but never recurse into the storage/cell graph.
    private static final String[] AE2_DRIVE_CALLBACK_FIELDS = {
            "callback",
            "activityCallback",
            "changeCallback",
            "statusChangeCallback",
            "listener",
            "activityListener",
            "onChange"
    };

    // Generic compat resolution is fail-soft and bounded. Depth alone is not
    // enough because branching owner graphs can grow exponentially.
    private static final int MAX_RESOLVE_OBJECTS = 48;

    private static final ThreadLocal<Token> AE2_TOKEN = new ThreadLocal<>();
    private static final ThreadLocal<ArrayDeque<DriveOperationFrame>> AE2_DRIVE_OPERATION_STACK =
            ThreadLocal.withInitial(ArrayDeque::new);
    private static final Map<Object, Boolean> AE2_DRIVES =
            Collections.synchronizedMap(new WeakHashMap<>());

    // Hot-path cache is intentionally session-scoped and identity-based. It
    // avoids WeakHashMap cleanup/hash overhead on every DriveWatcher operation
    // and is cleared when a profile starts/ends so it cannot retain a world.
    private static final Map<Object, ResolvedTarget> AE2_DRIVE_TARGET_CACHE =
            Collections.synchronizedMap(new IdentityHashMap<>());
    private static final Set<Object> AE2_DRIVE_TARGET_MISS_CACHE =
            Collections.newSetFromMap(Collections.synchronizedMap(new IdentityHashMap<>()));
    private static final Map<Class<?>, Field[]> AE2_DRIVE_OWNER_ACCESSORS =
            Collections.synchronizedMap(new IdentityHashMap<>());
    private static final Map<Class<?>, Field[]> AE2_DRIVE_CALLBACK_ACCESSORS =
            Collections.synchronizedMap(new IdentityHashMap<>());

    // AE2 15.5.0 DriveBlockEntity owns its live cell wrappers directly in
    // private final DriveWatcher[] invBySlot. Prefer building the reverse
    // watcher -> DriveBlockEntity map from that bounded array outside the hot
    // insert/extract path. GTOCore may transform callback shapes at runtime,
    // but this host-owned array is an exact, bounded ownership relation and
    // requires no recursive graph traversal or new mixin target.
    private static final int MAX_DRIVE_WATCHERS_PER_HOST = 64;
    private static final Set<Object> AE2_DRIVE_HOST_INDEXED =
            Collections.newSetFromMap(Collections.synchronizedMap(new IdentityHashMap<>()));
    private static volatile Field ae2DriveWatchersBySlotField;
    private static volatile boolean ae2DriveWatchersBySlotFieldMissing;

    // v20.3.2.7 typed stale-watcher resolver. Clean AE2 15.5.0 DriveWatcher
    // inherits the exact private DelegatingMEInventory.delegate field. For a
    // BasicCellInventory delegate, the exact private container field stores the
    // ISaveProvider lambda created by DriveBlockEntity.updateStateForSlot().
    // We inspect ONLY those two known fields and ONLY the provider's direct
    // captured fields. No methods, recursive resolver, or storage graph walk.
    private static final int MAX_DRIVE_DELEGATE_TYPE_DIAGNOSTICS = 12;
    private static volatile Field ae2DriveDelegateField;
    private static volatile boolean ae2DriveDelegateFieldMissing;
    private static volatile Field ae2BasicCellContainerField;
    private static volatile boolean ae2BasicCellContainerFieldMissing;
    private static final Map<String, Long> AE2_DRIVE_UNRESOLVED_DELEGATE_TYPES =
            Collections.synchronizedMap(new LinkedHashMap<>());

    // v20.3.2.7: session-local DriveWatcher coverage diagnostics. These are
    // primitive counters on the server hot path; resolver reason counters are
    // updated only on first resolution attempts because hits/misses are cached.
    private static long ae2DriveOperationBegins;
    private static long ae2DriveResolvedOperationBegins;
    private static long ae2DriveUnresolvedOperationBegins;
    private static long ae2DriveNestedSameDriveBegins;
    private static long ae2DriveTimedOperationBegins;
    private static long ae2DriveBeginExtract;
    private static long ae2DriveBeginInsert;
    private static long ae2DriveBeginPreferred;
    private static long ae2DriveBeginAvailableStacks;
    private static long ae2DriveBeginOther;
    private static long ae2DriveResolutionAttempts;
    private static long ae2DriveResolvedDirect;
    private static long ae2DriveResolvedCallback;
    private static long ae2DriveResolvedHostMapping;
    private static long ae2DriveHostMappingScans;
    private static long ae2DriveHostMappingWatchersSeen;
    private static long ae2DriveHostMappingAccessFailures;
    private static long ae2DriveCellResolutionAttempts;
    private static long ae2DriveResolvedCellSaveProvider;
    private static long ae2DriveCellDelegateBasic;
    private static long ae2DriveCellDelegateOther;
    private static long ae2DriveCellDelegateNull;
    private static long ae2DriveCellDelegateAccessFailures;
    private static long ae2DriveCellSaveProviderAccessFailures;
    private static long ae2DriveCellSaveProviderCaptureMisses;
    private static long ae2DriveUnresolvedNoOwnerAccessors;
    private static long ae2DriveUnresolvedNoCapturedDrive;
    private static long ae2DriveUnresolvedSafetyReject;
    private static long ae2DriveUnresolvedAccessFailure;
    private static long ae2DriveUnresolvedDirectOwnerNotDrive;
    private static long ae2DriveUnresolvedOther;
    private static long ae2DriveUnresolvedWithoutDirectAccessors;
    private static long ae2DriveUnresolvedWithoutCallbackAccessors;

    private static volatile Field ae2CurrentNodeField;
    private static volatile boolean ae2CurrentNodeFieldMissing;

    private CompatTiming() {
    }

    /**
     * Wrap a GTCEu/GTO scheduled runnable without modifying TaskHandler itself.
     *
     * GT Odyssey's GTOLib rejects mixins applied directly to TaskHandler.  Every
     * TaskHandler entry stores its work in TickableSubscription#runnable, so the
     * runnable can be wrapped one level earlier instead.
     */
    public static Runnable wrapGTRunnable(Runnable runnable) {
        if (runnable == null || runnable instanceof ProfiledGTRunnable) {
            return runnable;
        }
        return new ProfiledGTRunnable(runnable);
    }

    /** Begin profiling the AE2 node currently being ticked by TickManagerService. */
    public static void beginAE2Tick(Object tickManager) {
        AE2_TOKEN.remove();
        if (Props.notProcessing || tickManager == null) {
            return;
        }

        Object node = readAE2CurrentNode(tickManager);
        ResolvedTarget resolved = resolveTarget(node);
        if (resolved != null) {
            AE2GridProfiler.registerPhysicalDevice(resolved.blockEntity, resolved.name);
        }

        // The per-grid profiler uses the same node to aggregate all IGridTickable
        // work for the owning ME network. This does not replace the per-device
        // bucket; it is an additional inclusive grid view.
        AE2GridProfiler.beginDevice(node, resolved == null ? null : resolved.blockEntity);

        Token token = begin(resolved, true, "tick");
        if (token != null) {
            AE2_TOKEN.set(token);
        }
    }

    /** Finish profiling the AE2 node started by beginAE2Tick. */
    public static void endAE2Tick() {
        endAE2Tick(null);
    }

    /**
     * Finish profiling and record the TickRateModulation returned by AE2.
     * A single timestamp is captured first so profiler bookkeeping itself is
     * excluded from both the per-device and per-grid measurements.
     */
    public static void endAE2Tick(Object modulation) {
        long finishedAt = System.nanoTime();
        Token token = AE2_TOKEN.get();
        AE2_TOKEN.remove();
        try {
            endAt(token, finishedAt, spikeOperationForModulation(modulation));
        } finally {
            AE2GridProfiler.endDeviceAt(finishedAt, modulation);
        }
    }

    /**
     * Registers an AE2 ME Drive so non-ticking drives are still visible in an
     * Observable profile with 0 us/t when they did no storage work.
     */
    public static void registerAE2Drive(Object drive) {
        if (!(drive instanceof BlockEntity)) {
            return;
        }
        BlockEntity blockEntity = (BlockEntity) drive;
        if (!isAE2DriveBlockEntity(blockEntity)) {
            return;
        }
        AE2_DRIVES.put(drive, Boolean.TRUE);
        if (!Props.notProcessing) {
            ensurePassiveDriveEntry(blockEntity);
            indexDriveWatchersForSession(blockEntity);
        }
    }

    /**
     * Materializes currently loaded/known ME Drives immediately after
     * Observable clears its block timing map for a new session.
     */
    public static void onAE2CompatSessionStart() {
        clearDriveSessionCaches();
        resetDriveCoverageCounters();

        List<Object> drives;
        synchronized (AE2_DRIVES) {
            drives = new ArrayList<>(AE2_DRIVES.keySet());
        }
        for (Object drive : drives) {
            if (drive instanceof BlockEntity) {
                BlockEntity blockEntity = (BlockEntity) drive;
                ensurePassiveDriveEntry(blockEntity);
                indexDriveWatchersForSession(blockEntity);
            }
        }
    }

    /** Releases strong session-only DriveWatcher cache entries after snapshot. */
    public static void onAE2CompatSessionEnd() {
        clearDriveSessionCaches();
    }

    private static void clearDriveSessionCaches() {
        synchronized (AE2_DRIVE_TARGET_CACHE) {
            AE2_DRIVE_TARGET_CACHE.clear();
        }
        synchronized (AE2_DRIVE_TARGET_MISS_CACHE) {
            AE2_DRIVE_TARGET_MISS_CACHE.clear();
        }
        synchronized (AE2_DRIVE_HOST_INDEXED) {
            AE2_DRIVE_HOST_INDEXED.clear();
        }
        synchronized (AE2_DRIVE_UNRESOLVED_DELEGATE_TYPES) {
            AE2_DRIVE_UNRESOLVED_DELEGATE_TYPES.clear();
        }
    }

    private static void resetDriveCoverageCounters() {
        ae2DriveOperationBegins = 0L;
        ae2DriveResolvedOperationBegins = 0L;
        ae2DriveUnresolvedOperationBegins = 0L;
        ae2DriveNestedSameDriveBegins = 0L;
        ae2DriveTimedOperationBegins = 0L;
        ae2DriveBeginExtract = 0L;
        ae2DriveBeginInsert = 0L;
        ae2DriveBeginPreferred = 0L;
        ae2DriveBeginAvailableStacks = 0L;
        ae2DriveBeginOther = 0L;
        ae2DriveResolutionAttempts = 0L;
        ae2DriveResolvedDirect = 0L;
        ae2DriveResolvedCallback = 0L;
        ae2DriveResolvedHostMapping = 0L;
        ae2DriveHostMappingScans = 0L;
        ae2DriveHostMappingWatchersSeen = 0L;
        ae2DriveHostMappingAccessFailures = 0L;
        ae2DriveCellResolutionAttempts = 0L;
        ae2DriveResolvedCellSaveProvider = 0L;
        ae2DriveCellDelegateBasic = 0L;
        ae2DriveCellDelegateOther = 0L;
        ae2DriveCellDelegateNull = 0L;
        ae2DriveCellDelegateAccessFailures = 0L;
        ae2DriveCellSaveProviderAccessFailures = 0L;
        ae2DriveCellSaveProviderCaptureMisses = 0L;
        synchronized (AE2_DRIVE_UNRESOLVED_DELEGATE_TYPES) {
            AE2_DRIVE_UNRESOLVED_DELEGATE_TYPES.clear();
        }
        ae2DriveUnresolvedNoOwnerAccessors = 0L;
        ae2DriveUnresolvedNoCapturedDrive = 0L;
        ae2DriveUnresolvedSafetyReject = 0L;
        ae2DriveUnresolvedAccessFailure = 0L;
        ae2DriveUnresolvedDirectOwnerNotDrive = 0L;
        ae2DriveUnresolvedOther = 0L;
        ae2DriveUnresolvedWithoutDirectAccessors = 0L;
        ae2DriveUnresolvedWithoutCallbackAccessors = 0L;
    }

    private static void countDriveOperationBegin(String operation) {
        ae2DriveOperationBegins++;
        if ("drive.extract".equals(operation)) {
            ae2DriveBeginExtract++;
        } else if ("drive.insert".equals(operation)) {
            ae2DriveBeginInsert++;
        } else if ("drive.preferred".equals(operation)) {
            ae2DriveBeginPreferred++;
        } else if ("drive.availableStacks".equals(operation)) {
            ae2DriveBeginAvailableStacks++;
        } else {
            ae2DriveBeginOther++;
        }
    }

    public static DriveCoverageSnapshot snapshotAE2DriveCoverage() {
        int registeredDrives;
        synchronized (AE2_DRIVES) {
            registeredDrives = AE2_DRIVES.size();
        }

        int resolvedWatchers;
        int uniqueResolvedDrives;
        synchronized (AE2_DRIVE_TARGET_CACHE) {
            resolvedWatchers = AE2_DRIVE_TARGET_CACHE.size();
            Set<BlockEntity> unique = Collections.newSetFromMap(new IdentityHashMap<>());
            for (ResolvedTarget target : AE2_DRIVE_TARGET_CACHE.values()) {
                if (target != null && target.blockEntity != null) {
                    unique.add(target.blockEntity);
                }
            }
            uniqueResolvedDrives = unique.size();
        }

        int unresolvedWatchers;
        synchronized (AE2_DRIVE_TARGET_MISS_CACHE) {
            unresolvedWatchers = AE2_DRIVE_TARGET_MISS_CACHE.size();
        }

        Map<String, Long> unresolvedDelegateTypes;
        synchronized (AE2_DRIVE_UNRESOLVED_DELEGATE_TYPES) {
            unresolvedDelegateTypes = new LinkedHashMap<>(AE2_DRIVE_UNRESOLVED_DELEGATE_TYPES);
        }

        return new DriveCoverageSnapshot(
                registeredDrives, resolvedWatchers, unresolvedWatchers, uniqueResolvedDrives,
                ae2DriveOperationBegins, ae2DriveResolvedOperationBegins, ae2DriveUnresolvedOperationBegins,
                ae2DriveNestedSameDriveBegins, ae2DriveTimedOperationBegins,
                ae2DriveBeginExtract, ae2DriveBeginInsert, ae2DriveBeginPreferred,
                ae2DriveBeginAvailableStacks, ae2DriveBeginOther,
                ae2DriveResolutionAttempts, ae2DriveResolvedDirect, ae2DriveResolvedCallback,
                ae2DriveResolvedHostMapping, ae2DriveHostMappingScans,
                ae2DriveHostMappingWatchersSeen, ae2DriveHostMappingAccessFailures,
                ae2DriveCellResolutionAttempts, ae2DriveResolvedCellSaveProvider,
                ae2DriveCellDelegateBasic, ae2DriveCellDelegateOther, ae2DriveCellDelegateNull,
                ae2DriveCellDelegateAccessFailures, ae2DriveCellSaveProviderAccessFailures,
                ae2DriveCellSaveProviderCaptureMisses, unresolvedDelegateTypes,
                ae2DriveUnresolvedNoOwnerAccessors, ae2DriveUnresolvedNoCapturedDrive,
                ae2DriveUnresolvedSafetyReject, ae2DriveUnresolvedAccessFailure,
                ae2DriveUnresolvedDirectOwnerNotDrive, ae2DriveUnresolvedOther,
                ae2DriveUnresolvedWithoutDirectAccessors, ae2DriveUnresolvedWithoutCallbackAccessors);
    }

    /** Begin one top-level DriveWatcher storage operation. */
    public static void beginAE2DriveOperation(Object driveWatcher) {
        beginAE2DriveOperation(driveWatcher, "drive");
    }

    /** Begin one named top-level DriveWatcher storage operation for v20.3.1 operation-aware spikes. */
    public static void beginAE2DriveOperation(Object driveWatcher, String operation) {
        ArrayDeque<DriveOperationFrame> stack = AE2_DRIVE_OPERATION_STACK.get();
        if (Props.notProcessing || driveWatcher == null) {
            stack.push(DriveOperationFrame.EMPTY);
            return;
        }

        countDriveOperationBegin(operation);
        ResolvedTarget target = resolveDriveWatcherTarget(driveWatcher);
        if (target == null || !isAE2DriveBlockEntity(target.blockEntity)) {
            ae2DriveUnresolvedOperationBegins++;
            stack.push(DriveOperationFrame.EMPTY);
            return;
        }
        ae2DriveResolvedOperationBegins++;

        registerAE2Drive(target.blockEntity);

        // DriveWatcher methods can delegate to one another. Only measure the
        // outermost call for a drive so insert/extract/getAvailableStacks do
        // not double-count nested watcher work.
        boolean nestedSameDrive = false;
        for (DriveOperationFrame frame : stack) {
            if (frame.target != null && frame.target.blockEntity == target.blockEntity) {
                nestedSameDrive = true;
                break;
            }
        }

        if (nestedSameDrive) {
            ae2DriveNestedSameDriveBegins++;
        }
        Token token = nestedSameDrive ? null : begin(target, true, operation);
        if (token != null) {
            ae2DriveTimedOperationBegins++;
        }
        stack.push(new DriveOperationFrame(target, token));
    }

    /** Finish the DriveWatcher operation started by beginAE2DriveOperation. */
    public static void endAE2DriveOperation() {
        long finishedAt = System.nanoTime();
        ArrayDeque<DriveOperationFrame> stack = AE2_DRIVE_OPERATION_STACK.get();
        if (stack.isEmpty()) {
            return;
        }
        DriveOperationFrame frame = stack.pop();
        endAt(frame.token, finishedAt);
        if (stack.isEmpty()) {
            AE2_DRIVE_OPERATION_STACK.remove();
        }
    }

    private static void ensurePassiveDriveEntry(BlockEntity blockEntity) {
        if (blockEntity == null || blockEntity.isRemoved()) {
            return;
        }
        Level level = blockEntity.getLevel();
        if (!(level instanceof ServerLevel)) {
            return;
        }
        String name = blockName(blockEntity);
        Observable.INSTANCE.getPROFILER().processCompatBlockEntity(blockEntity, level, name);
        AE2GridProfiler.registerPhysicalDevice(blockEntity, name);
    }

    private static boolean isAE2DriveBlockEntity(BlockEntity blockEntity) {
        return blockEntity != null
                && "appeng.blockentity.storage.DriveBlockEntity".equals(blockEntity.getClass().getName());
    }

    private static void indexDriveWatchersForSession(BlockEntity blockEntity) {
        if (!isAE2DriveBlockEntity(blockEntity) || blockEntity.isRemoved()) {
            return;
        }
        if (AE2_DRIVE_HOST_INDEXED.contains(blockEntity)) {
            return;
        }

        Field field = driveWatchersBySlotField(blockEntity.getClass());
        if (field == null) {
            return;
        }

        ae2DriveHostMappingScans++;
        try {
            Object value = field.get(blockEntity);
            if (!(value instanceof Object[])) {
                return;
            }
            Object[] watchers = (Object[]) value;
            int limit = Math.min(watchers.length, MAX_DRIVE_WATCHERS_PER_HOST);
            ResolvedTarget target = new ResolvedTarget(blockEntity, blockName(blockEntity));
            int seen = 0;
            for (int i = 0; i < limit; i++) {
                Object watcher = watchers[i];
                if (watcher == null || !isAE2DriveWatcher(watcher.getClass())) {
                    continue;
                }
                seen++;
                ae2DriveHostMappingWatchersSeen++;
                ResolvedTarget previous = AE2_DRIVE_TARGET_CACHE.put(watcher, target);
                AE2_DRIVE_TARGET_MISS_CACHE.remove(watcher);
                if (previous == null) {
                    ae2DriveResolvedHostMapping++;
                }
            }
            // Constructor-time registration sees an allocated but still empty
            // invBySlot array. Only mark the host indexed after at least one
            // live watcher was observed so the existing onReady registration
            // can populate the map after AE2 creates its cell wrappers.
            if (seen > 0) {
                AE2_DRIVE_HOST_INDEXED.add(blockEntity);
            }
        } catch (Throwable ignored) {
            ae2DriveHostMappingAccessFailures++;
        }
    }

    private static Field driveWatchersBySlotField(Class<?> driveType) {
        if (driveType == null || ae2DriveWatchersBySlotFieldMissing) {
            return null;
        }
        Field cached = ae2DriveWatchersBySlotField;
        if (cached != null) {
            return cached;
        }

        synchronized (CompatTiming.class) {
            cached = ae2DriveWatchersBySlotField;
            if (cached != null || ae2DriveWatchersBySlotFieldMissing) {
                return cached;
            }
            Class<?> type = driveType;
            while (type != null && type != Object.class) {
                try {
                    Field named = type.getDeclaredField("invBySlot");
                    if (isDriveWatcherArrayField(named)) {
                        named.setAccessible(true);
                        ae2DriveWatchersBySlotField = named;
                        return named;
                    }
                } catch (NoSuchFieldException ignored) {
                    // Fall through to the bounded type-based lookup below.
                } catch (Throwable ignored) {
                    ae2DriveWatchersBySlotFieldMissing = true;
                    return null;
                }

                // GTOCore may alter field names while retaining the AE2 field
                // type. Accept exactly one DriveWatcher[] field; never inspect
                // the array contents recursively.
                Field candidate = null;
                try {
                    for (Field field : type.getDeclaredFields()) {
                        if (!isDriveWatcherArrayField(field)) {
                            continue;
                        }
                        if (candidate != null) {
                            ae2DriveWatchersBySlotFieldMissing = true;
                            return null;
                        }
                        candidate = field;
                    }
                    if (candidate != null) {
                        candidate.setAccessible(true);
                        ae2DriveWatchersBySlotField = candidate;
                        return candidate;
                    }
                } catch (Throwable ignored) {
                    ae2DriveWatchersBySlotFieldMissing = true;
                    return null;
                }
                type = type.getSuperclass();
            }
            ae2DriveWatchersBySlotFieldMissing = true;
            return null;
        }
    }

    private static boolean isDriveWatcherArrayField(Field field) {
        if (field == null || !field.getType().isArray()) {
            return false;
        }
        Class<?> component = field.getType().getComponentType();
        return component != null && "appeng.me.storage.DriveWatcher".equals(component.getName());
    }

    private static ResolvedTarget resolveDriveWatcherTarget(Object watcher) {
        if (watcher == null || !isAE2DriveWatcher(watcher.getClass())) {
            return null;
        }

        ResolvedTarget cached = AE2_DRIVE_TARGET_CACHE.get(watcher);
        if (cached != null) {
            if (!cached.blockEntity.isRemoved()) {
                return cached;
            }
            AE2_DRIVE_TARGET_CACHE.remove(watcher);
        }
        if (AE2_DRIVE_TARGET_MISS_CACHE.contains(watcher)) {
            return null;
        }

        // IMPORTANT: no generic resolveTarget(watcher) fallback here. A
        // DriveWatcher may reference storage handlers with deep/cyclic owner
        // graphs. Scanning those on every first-seen cell caused the v20.3.2
        // watchdog hang on large servers. Only inspect direct owner candidates.
        ae2DriveResolutionAttempts++;
        DriveResolutionProbe probe = new DriveResolutionProbe();
        ResolvedTarget resolved = resolveDriveWatcherDirectOwner(watcher, probe);
        if (resolved != null) {
            ae2DriveResolvedDirect++;
            AE2_DRIVE_TARGET_CACHE.put(watcher, resolved);
            return resolved;
        }

        resolved = resolveDriveWatcherCallbackOwner(watcher, probe);
        if (resolved != null) {
            ae2DriveResolvedCallback++;
            AE2_DRIVE_TARGET_CACHE.put(watcher, resolved);
            return resolved;
        }

        // v20.3.2.7: stale/unmounted DriveWatcher instances may no longer be
        // present in DriveBlockEntity.invBySlot. Recover their host only via
        // the exact AE2 15.5.0 typed chain:
        // DriveWatcher -> DelegatingMEInventory.delegate (BasicCellInventory)
        // -> BasicCellInventory.container (ISaveProvider) -> direct lambda capture.
        // No arbitrary fields or methods are followed.
        resolved = resolveDriveWatcherCellOwner(watcher);
        if (resolved != null) {
            ae2DriveResolvedCellSaveProvider++;
            AE2_DRIVE_TARGET_CACHE.put(watcher, resolved);
            return resolved;
        }

        if (!probe.directAccessorPresent) {
            ae2DriveUnresolvedWithoutDirectAccessors++;
        }
        if (!probe.callbackAccessorPresent) {
            ae2DriveUnresolvedWithoutCallbackAccessors++;
        }
        switch (probe.unresolvedReason()) {
            case NO_OWNER_ACCESSORS -> ae2DriveUnresolvedNoOwnerAccessors++;
            case NO_CAPTURED_DRIVE -> ae2DriveUnresolvedNoCapturedDrive++;
            case SAFETY_REJECT -> ae2DriveUnresolvedSafetyReject++;
            case ACCESS_FAILURE -> ae2DriveUnresolvedAccessFailure++;
            case DIRECT_OWNER_NOT_DRIVE -> ae2DriveUnresolvedDirectOwnerNotDrive++;
            default -> ae2DriveUnresolvedOther++;
        }
        AE2_DRIVE_TARGET_MISS_CACHE.add(watcher);
        return null;
    }

    private static ResolvedTarget resolveDriveWatcherCellOwner(Object watcher) {
        ae2DriveCellResolutionAttempts++;
        Field delegateField = driveWatcherDelegateField(watcher.getClass());
        if (delegateField == null) {
            ae2DriveCellDelegateAccessFailures++;
            recordUnresolvedDriveDelegateType("<delegate-field-unavailable>");
            return null;
        }

        final Object delegate;
        try {
            delegate = delegateField.get(watcher);
        } catch (Throwable ignored) {
            ae2DriveCellDelegateAccessFailures++;
            recordUnresolvedDriveDelegateType("<delegate-access-failure>");
            return null;
        }

        if (delegate == null) {
            ae2DriveCellDelegateNull++;
            recordUnresolvedDriveDelegateType("<null>");
            return null;
        }
        if (!"appeng.me.cells.BasicCellInventory".equals(delegate.getClass().getName())) {
            ae2DriveCellDelegateOther++;
            recordUnresolvedDriveDelegateType(delegate.getClass().getName());
            return null;
        }
        ae2DriveCellDelegateBasic++;

        Field containerField = basicCellContainerField(delegate.getClass());
        if (containerField == null) {
            ae2DriveCellSaveProviderAccessFailures++;
            return null;
        }

        final Object saveProvider;
        try {
            saveProvider = containerField.get(delegate);
        } catch (Throwable ignored) {
            ae2DriveCellSaveProviderAccessFailures++;
            return null;
        }
        if (saveProvider == null) {
            ae2DriveCellSaveProviderCaptureMisses++;
            return null;
        }

        ResolvedTarget resolved = resolveDriveFromTypedSaveProviderCapture(saveProvider);
        if (resolved == null) {
            ae2DriveCellSaveProviderCaptureMisses++;
        }
        return resolved;
    }

    private static Field driveWatcherDelegateField(Class<?> watcherType) {
        if (watcherType == null || ae2DriveDelegateFieldMissing) {
            return null;
        }
        Field cached = ae2DriveDelegateField;
        if (cached != null) {
            return cached;
        }
        synchronized (CompatTiming.class) {
            cached = ae2DriveDelegateField;
            if (cached != null || ae2DriveDelegateFieldMissing) {
                return cached;
            }
            for (Class<?> type = watcherType; type != null && type != Object.class; type = type.getSuperclass()) {
                if (!"appeng.me.storage.DelegatingMEInventory".equals(type.getName())) {
                    continue;
                }
                try {
                    Field field = type.getDeclaredField("delegate");
                    if (!"appeng.api.storage.MEStorage".equals(field.getType().getName())) {
                        ae2DriveDelegateFieldMissing = true;
                        return null;
                    }
                    field.setAccessible(true);
                    ae2DriveDelegateField = field;
                    return field;
                } catch (Throwable ignored) {
                    ae2DriveDelegateFieldMissing = true;
                    return null;
                }
            }
            ae2DriveDelegateFieldMissing = true;
            return null;
        }
    }

    private static Field basicCellContainerField(Class<?> cellType) {
        if (cellType == null || ae2BasicCellContainerFieldMissing
                || !"appeng.me.cells.BasicCellInventory".equals(cellType.getName())) {
            return null;
        }
        Field cached = ae2BasicCellContainerField;
        if (cached != null) {
            return cached;
        }
        synchronized (CompatTiming.class) {
            cached = ae2BasicCellContainerField;
            if (cached != null || ae2BasicCellContainerFieldMissing) {
                return cached;
            }
            try {
                Field field = cellType.getDeclaredField("container");
                if (!"appeng.api.storage.cells.ISaveProvider".equals(field.getType().getName())) {
                    ae2BasicCellContainerFieldMissing = true;
                    return null;
                }
                field.setAccessible(true);
                ae2BasicCellContainerField = field;
                return field;
            } catch (Throwable ignored) {
                ae2BasicCellContainerFieldMissing = true;
                return null;
            }
        }
    }

    private static ResolvedTarget resolveDriveFromTypedSaveProviderCapture(Object saveProvider) {
        Class<?> providerType = saveProvider.getClass();
        String providerName = providerType.getName();
        if (!providerType.isSynthetic()
                && !providerName.contains("$$Lambda$")
                && !providerName.contains("$Lambda")) {
            return null;
        }

        int inspected = 0;
        for (Field captured : providerType.getDeclaredFields()) {
            if (Modifier.isStatic(captured.getModifiers()) || inspected++ >= 4) {
                continue;
            }
            try {
                captured.setAccessible(true);
                Object value = captured.get(saveProvider);
                if (!(value instanceof BlockEntity)) {
                    continue;
                }
                BlockEntity blockEntity = (BlockEntity) value;
                if (isAE2DriveBlockEntity(blockEntity) && !blockEntity.isRemoved()) {
                    return new ResolvedTarget(blockEntity, blockName(blockEntity));
                }
            } catch (Throwable ignored) {
                ae2DriveCellSaveProviderAccessFailures++;
                // Direct capture only. Never recurse into provider values.
            }
        }
        return null;
    }

    private static void recordUnresolvedDriveDelegateType(String typeName) {
        String key = typeName == null || typeName.isEmpty() ? "<unknown>" : typeName;
        synchronized (AE2_DRIVE_UNRESOLVED_DELEGATE_TYPES) {
            Long count = AE2_DRIVE_UNRESOLVED_DELEGATE_TYPES.get(key);
            if (count != null) {
                AE2_DRIVE_UNRESOLVED_DELEGATE_TYPES.put(key, count + 1L);
                return;
            }
            if (AE2_DRIVE_UNRESOLVED_DELEGATE_TYPES.size() < MAX_DRIVE_DELEGATE_TYPE_DIAGNOSTICS) {
                AE2_DRIVE_UNRESOLVED_DELEGATE_TYPES.put(key, 1L);
                return;
            }
            String overflow = "<other-delegate-types>";
            AE2_DRIVE_UNRESOLVED_DELEGATE_TYPES.put(overflow,
                    AE2_DRIVE_UNRESOLVED_DELEGATE_TYPES.getOrDefault(overflow, 0L) + 1L);
        }
    }

    private static ResolvedTarget resolveDriveWatcherDirectOwner(Object watcher) {
        return resolveDriveWatcherDirectOwner(watcher, null);
    }

    private static ResolvedTarget resolveDriveWatcherDirectOwner(Object watcher, DriveResolutionProbe probe) {
        Field[] accessors = driveOwnerAccessors(watcher.getClass());
        if (probe != null) {
            probe.directAccessorPresent = accessors.length > 0;
        }
        for (Field field : accessors) {
            try {
                Object owner = field.get(watcher);
                if (probe != null) {
                    probe.anyFieldRead = true;
                }
                if (owner instanceof BlockEntity) {
                    BlockEntity blockEntity = (BlockEntity) owner;
                    if (isAE2DriveBlockEntity(blockEntity)) {
                        return new ResolvedTarget(blockEntity, blockName(blockEntity));
                    }
                    if (probe != null) {
                        probe.sawNonDriveBlockEntity = true;
                    }
                } else if (owner != null && probe != null) {
                    probe.sawDirectNonDriveValue = true;
                }
            } catch (IllegalAccessException | RuntimeException ignored) {
                if (probe != null) {
                    probe.accessFailure = true;
                }
                // Fail soft. An inaccessible owner means this watcher is skipped.
            }
        }
        return null;
    }

    private static ResolvedTarget resolveDriveWatcherCallbackOwner(Object watcher, DriveResolutionProbe probe) {
        Field[] accessors = driveCallbackAccessors(watcher.getClass());
        probe.callbackAccessorPresent = accessors.length > 0;
        for (Field field : accessors) {
            try {
                Object callback = field.get(watcher);
                probe.anyFieldRead = true;
                ResolvedTarget resolved = resolveDriveFromDirectCapture(callback, probe);
                if (resolved != null) {
                    return resolved;
                }
            } catch (IllegalAccessException | RuntimeException ignored) {
                probe.accessFailure = true;
                // Fail soft. The watcher remains unprofiled rather than widening the reflection search.
            }
        }
        return null;
    }

    private static ResolvedTarget resolveDriveFromDirectCapture(Object callback, DriveResolutionProbe probe) {
        if (callback == null) {
            return null;
        }
        probe.sawCallbackValue = true;
        if (callback instanceof BlockEntity) {
            BlockEntity blockEntity = (BlockEntity) callback;
            if (isAE2DriveBlockEntity(blockEntity)) {
                return new ResolvedTarget(blockEntity, blockName(blockEntity));
            }
            probe.sawNonDriveBlockEntity = true;
            return null;
        }

        Class<?> callbackType = callback.getClass();
        String callbackClassName = callbackType.getName();
        if (!callbackType.isSynthetic()
                && !callbackClassName.contains("$$Lambda$")
                && !callbackClassName.contains("$Lambda")) {
            probe.safetyRejectedCallbackShape = true;
            return null;
        }

        int inspected = 0;
        for (Field captured : callbackType.getDeclaredFields()) {
            if (Modifier.isStatic(captured.getModifiers()) || inspected++ >= 8) {
                continue;
            }
            try {
                captured.setAccessible(true);
                Object value = captured.get(callback);
                probe.anyFieldRead = true;
                if (value instanceof BlockEntity) {
                    BlockEntity blockEntity = (BlockEntity) value;
                    if (isAE2DriveBlockEntity(blockEntity)) {
                        return new ResolvedTarget(blockEntity, blockName(blockEntity));
                    }
                    probe.sawNonDriveBlockEntity = true;
                }
            } catch (IllegalAccessException | RuntimeException ignored) {
                probe.accessFailure = true;
                // Direct capture only. Never recurse into callback values.
            }
        }
        return null;
    }

    private static Field[] driveCallbackAccessors(Class<?> watcherType) {
        Field[] cached = AE2_DRIVE_CALLBACK_ACCESSORS.get(watcherType);
        if (cached != null) {
            return cached;
        }

        List<Field> result = new ArrayList<>();
        for (String fieldName : AE2_DRIVE_CALLBACK_FIELDS) {
            Field field = findInstanceField(watcherType, fieldName);
            if (field != null && !result.contains(field)) {
                result.add(field);
            }
        }

        // One-time metadata fallback: Runnable fields and explicitly callback/listener-named
        // interface fields are safe candidates. Their values are still inspected only one hop.
        for (Class<?> current = watcherType; current != null && current != Object.class; current = current.getSuperclass()) {
            for (Field field : current.getDeclaredFields()) {
                if (Modifier.isStatic(field.getModifiers()) || result.contains(field) || result.size() >= 4) {
                    continue;
                }
                Class<?> fieldType = field.getType();
                String fieldName = field.getName().toLowerCase(java.util.Locale.ROOT);
                boolean callbackLike = Runnable.class.isAssignableFrom(fieldType)
                        || (fieldType.isInterface()
                        && (fieldName.contains("callback") || fieldName.contains("listener")
                        || fieldName.contains("activity") || fieldName.contains("change")));
                if (!callbackLike) {
                    continue;
                }
                try {
                    field.setAccessible(true);
                    result.add(field);
                } catch (RuntimeException ignored) {
                    // Ignore inaccessible optional fields.
                }
            }
        }

        Field[] built = result.toArray(new Field[0]);
        AE2_DRIVE_CALLBACK_ACCESSORS.put(watcherType, built);
        return built;
    }

    private static Field[] driveOwnerAccessors(Class<?> watcherType) {
        Field[] cached = AE2_DRIVE_OWNER_ACCESSORS.get(watcherType);
        if (cached != null) {
            return cached;
        }

        List<Field> result = new ArrayList<>();

        // First prefer known owner field names. This prevents us from touching
        // cell/storage-handler fields that can lead into very large graphs.
        for (String fieldName : AE2_DRIVE_OWNER_FIELDS) {
            Field field = findInstanceField(watcherType, fieldName);
            if (field != null && !result.contains(field)) {
                result.add(field);
            }
        }

        // Version/fork fallback: only fields whose DECLARED TYPE itself looks
        // like the drive host. This scan happens once per DriveWatcher class.
        for (Class<?> current = watcherType; current != null && current != Object.class; current = current.getSuperclass()) {
            for (Field field : current.getDeclaredFields()) {
                if (Modifier.isStatic(field.getModifiers()) || result.contains(field)) {
                    continue;
                }
                Class<?> fieldType = field.getType();
                String typeName = fieldType.getName();
                if (!BlockEntity.class.isAssignableFrom(fieldType)
                        && !typeName.endsWith(".IChestOrDrive")
                        && !typeName.endsWith(".DriveBlockEntity")) {
                    continue;
                }
                try {
                    field.setAccessible(true);
                    result.add(field);
                } catch (RuntimeException ignored) {
                    // Ignore inaccessible optional fields.
                }
            }
        }

        Field[] built = result.toArray(new Field[0]);
        AE2_DRIVE_OWNER_ACCESSORS.put(watcherType, built);
        return built;
    }

    private static Field findInstanceField(Class<?> type, String fieldName) {
        for (Class<?> current = type; current != null && current != Object.class; current = current.getSuperclass()) {
            try {
                Field field = current.getDeclaredField(fieldName);
                if (Modifier.isStatic(field.getModifiers())) {
                    return null;
                }
                field.setAccessible(true);
                return field;
            } catch (NoSuchFieldException ignored) {
                // Keep walking the hierarchy.
            } catch (RuntimeException ignored) {
                return null;
            }
        }
        return null;
    }

    private static void runProfiled(ResolvedTarget target, Runnable runnable) {
        Token token = begin(target, false, null);
        if (token == null) {
            runnable.run();
            return;
        }

        try {
            runnable.run();
        } finally {
            end(token);
        }
    }

    private static Token begin(ResolvedTarget target, boolean captureAE2Spike, String spikeOperation) {
        if (target == null || Props.notProcessing) {
            return null;
        }

        BlockEntity blockEntity = target.blockEntity;
        if (blockEntity.isRemoved()) {
            return null;
        }

        Level level = blockEntity.getLevel();
        if (!(level instanceof ServerLevel)) {
            return null;
        }

        Profiler profiler = Observable.INSTANCE.getPROFILER();
        Profiler.TimingData data = profiler.processCompatBlockEntity(blockEntity, level, target.name);

        boolean tagged = false;
        Profiler.TimingData previousTarget = null;
        try {
            tagged = Thread.currentThread() == profiler.getServerThread();
        } catch (Throwable ignored) {
            // serverThread is lateinit. Compat must fail soft if profiling starts unusually early.
        }

        if (tagged) {
            previousTarget = Props.currentTarget.getAndSet(data);
        }

        return new Token(data, blockEntity, captureAE2Spike, spikeOperation, System.nanoTime(), tagged, previousTarget);
    }

    private static void end(Token token) {
        endAt(token, System.nanoTime());
    }

    private static void endAt(Token token, long finishedAt) {
        endAt(token, finishedAt, null);
    }

    private static void endAt(Token token, long finishedAt, String spikeOperationOverride) {
        if (token == null) {
            return;
        }

        long elapsed = finishedAt - token.startedAt;
        synchronized (token.data) {
            token.data.setTime(token.data.getTime() + elapsed);
            token.data.setTicks(token.data.getTicks() + 1);
        }

        if (token.captureAE2Spike) {
            String operation = spikeOperationOverride == null ? token.spikeOperation : spikeOperationOverride;
            AE2GridProfiler.recordPhysicalSpike(token.blockEntity, elapsed, operation);
        }

        if (token.tagged) {
            Props.currentTarget.set(token.previousTarget);
        }
    }

    private static String spikeOperationForModulation(Object modulation) {
        if (modulation instanceof Enum<?>) {
            // Enum#name returns the stable declared name without allocating a formatted label.
            return ((Enum<?>) modulation).name();
        }
        return "tick";
    }

    private static Object readAE2CurrentNode(Object tickManager) {
        if (ae2CurrentNodeFieldMissing) {
            return null;
        }

        Field field = ae2CurrentNodeField;
        if (field == null) {
            synchronized (CompatTiming.class) {
                field = ae2CurrentNodeField;
                if (field == null && !ae2CurrentNodeFieldMissing) {
                    try {
                        field = tickManager.getClass().getDeclaredField("currentlyTicking");
                        field.setAccessible(true);
                        ae2CurrentNodeField = field;
                    } catch (ReflectiveOperationException | RuntimeException ignored) {
                        ae2CurrentNodeFieldMissing = true;
                        return null;
                    }
                }
            }
        }

        try {
            return field.get(tickManager);
        } catch (IllegalAccessException ignored) {
            return null;
        }
    }

    /** Package-private resolver reused by the AE2 grid profiler. */
    static BlockEntity resolveBlockEntityForCompat(Object source) {
        ResolvedTarget resolved = resolveTarget(source);
        return resolved == null ? null : resolved.blockEntity;
    }

    private static ResolvedTarget resolveTarget(Object source) {
        Set<Object> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        int[] remainingObjects = {MAX_RESOLVE_OBJECTS};
        return resolveTarget(source, visited, 0, remainingObjects);
    }

    private static ResolvedTarget resolveTarget(
            Object source,
            Set<Object> visited,
            int depth,
            int[] remainingObjects
    ) {
        if (source == null || depth > 8 || remainingObjects[0]-- <= 0 || !visited.add(source)) {
            return null;
        }

        if (source instanceof BlockEntity) {
            BlockEntity blockEntity = (BlockEntity) source;
            return new ResolvedTarget(blockEntity, blockName(blockEntity));
        }

        Class<?> type = source.getClass();
        if (!isCompatType(type) && !isCaptureCarrier(type)) {
            return null;
        }

        // Never let the generic resolver fan out from DriveWatcher. Its direct
        // host lookup is deliberately narrow and cached for server safety.
        if (isAE2DriveWatcher(type)) {
            return resolveDriveWatcherDirectOwner(source);
        }

        String partName = partName(source);

        for (String methodName : OWNER_METHODS) {
            Object child = invokeNoArg(source, methodName);
            if (child == null || child == source) {
                continue;
            }

            ResolvedTarget resolved = resolveTarget(child, visited, depth + 1, remainingObjects);
            if (resolved != null) {
                if (partName != null) {
                    return new ResolvedTarget(resolved.blockEntity, partName);
                }
                return resolved;
            }
        }

        // GTCEu machine traits frequently expose their MetaMachine as a field rather
        // than a no-arg getter. Follow only a short allow-list to avoid walking an
        // arbitrary mod object graph.
        if (isCompatType(type)) {
            for (String fieldName : OWNER_FIELDS) {
                Object child = readField(source, fieldName);
                if (child == null || child == source) {
                    continue;
                }

                ResolvedTarget resolved = resolveTarget(child, visited, depth + 1, remainingObjects);
                if (resolved != null) {
                    if (partName != null) {
                        return new ResolvedTarget(resolved.blockEntity, partName);
                    }
                    return resolved;
                }
            }
        }

        // Lambda/method-reference objects hold their captured receiver/arguments in
        // synthetic instance fields. This is how RecipeLogic::serverTick and
        // MetaMachineBlockEntity method references are mapped back to the machine.
        if (isCaptureCarrier(type)) {
            for (Field field : type.getDeclaredFields()) {
                if (Modifier.isStatic(field.getModifiers())) {
                    continue;
                }

                try {
                    field.setAccessible(true);
                    Object captured = field.get(source);
                    ResolvedTarget resolved = resolveTarget(captured, visited, depth + 1, remainingObjects);
                    if (resolved != null) {
                        return resolved;
                    }
                } catch (IllegalAccessException | RuntimeException ignored) {
                    // Keep searching other captured values.
                }
            }
        }

        return null;
    }

    private static Object invokeNoArg(Object source, String methodName) {
        try {
            Method method = source.getClass().getMethod(methodName);
            if (method.getParameterCount() != 0) {
                return null;
            }
            return method.invoke(source);
        } catch (ReflectiveOperationException | RuntimeException ignored) {
            return null;
        }
    }

    private static Object readField(Object source, String fieldName) {
        Class<?> type = source.getClass();
        while (type != null && type != Object.class) {
            try {
                Field field = type.getDeclaredField(fieldName);
                if (Modifier.isStatic(field.getModifiers())) {
                    return null;
                }
                field.setAccessible(true);
                return field.get(source);
            } catch (NoSuchFieldException ignored) {
                type = type.getSuperclass();
            } catch (IllegalAccessException | RuntimeException ignored) {
                return null;
            }
        }
        return null;
    }

    private static String partName(Object source) {
        // For an actual IPart, getPartItem() is the most precise identity.
        // Third-party AE2 addons (ExtendedAE, etc.) commonly subclass AE2 part
        // classes, so do not assume the concrete class itself lives in appeng.*.
        Object partItem = invokeNoArg(source, "getPartItem");
        String itemName = itemName(partItem);
        if (itemName != null) {
            return itemName;
        }

        // IGridNode exposes a visual representation specifically intended to
        // identify the node in UIs. This is especially useful for addon parts:
        // the node itself is appeng.*, while its owner may be com.glodblock.*.
        Object visual = invokeNoArg(source, "getVisualRepresentation");
        if (visual != null) {
            Object item = invokeNoArg(visual, "getItem");
            itemName = itemName(item);
            if (itemName != null) {
                return itemName;
            }
        }

        return null;
    }

    private static String itemName(Object value) {
        if (value instanceof Item) {
            return BuiltInRegistries.ITEM.getKey((Item) value).toString();
        }

        if (value != null) {
            Object item = invokeNoArg(value, "asItem");
            if (item instanceof Item) {
                return BuiltInRegistries.ITEM.getKey((Item) item).toString();
            }
        }
        return null;
    }

    private static String blockName(BlockEntity blockEntity) {
        return BuiltInRegistries.BLOCK.getKey(blockEntity.getBlockState().getBlock()).toString();
    }

    private static boolean isCompatType(Class<?> type) {
        // Check the complete hierarchy, not only the concrete class name.
        // ExtendedAE's PartExImportBus for example is com.glodblock.*, but it
        // subclasses/implements AE2 part types and must be followed back to its
        // CableBusBlockEntity.
        for (Class<?> current = type; current != null && current != Object.class; current = current.getSuperclass()) {
            if (isCompatClassName(current.getName()) || hasCompatInterface(current)) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasCompatInterface(Class<?> type) {
        for (Class<?> iface : type.getInterfaces()) {
            if (isCompatClassName(iface.getName()) || hasCompatInterface(iface)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isCompatClassName(String name) {
        return name.startsWith("appeng.")
                || name.startsWith("com.gregtechceu.")
                || name.startsWith("com.gtocore.")
                || name.startsWith("com.gtolib.");
    }

    private static boolean isAE2DriveWatcher(Class<?> type) {
        return type != null && "appeng.me.storage.DriveWatcher".equals(type.getName());
    }

    private static boolean isCaptureCarrier(Class<?> type) {
        String name = type.getName();
        return type.isSynthetic() || type.isAnonymousClass() || name.contains("$$Lambda$");
    }

    private static final class ProfiledGTRunnable implements Runnable {
        private final Runnable delegate;
        private volatile ResolvedTarget resolved;

        private ProfiledGTRunnable(Runnable delegate) {
            this.delegate = delegate;
        }

        @Override
        public void run() {
            if (Props.notProcessing) {
                delegate.run();
                return;
            }

            ResolvedTarget target = resolved;
            if (target == null || target.blockEntity.isRemoved()) {
                target = resolveTarget(delegate);
                if (target != null) {
                    resolved = target;
                }
            }

            runProfiled(target, delegate);
        }
    }

    private enum DriveUnresolvedReason {
        NO_OWNER_ACCESSORS,
        NO_CAPTURED_DRIVE,
        SAFETY_REJECT,
        ACCESS_FAILURE,
        DIRECT_OWNER_NOT_DRIVE,
        OTHER
    }

    private static final class DriveResolutionProbe {
        private boolean directAccessorPresent;
        private boolean callbackAccessorPresent;
        private boolean anyFieldRead;
        private boolean accessFailure;
        private boolean sawCallbackValue;
        private boolean safetyRejectedCallbackShape;
        private boolean sawNonDriveBlockEntity;
        private boolean sawDirectNonDriveValue;

        private DriveUnresolvedReason unresolvedReason() {
            if (!directAccessorPresent && !callbackAccessorPresent) {
                return DriveUnresolvedReason.NO_OWNER_ACCESSORS;
            }
            if (accessFailure && !anyFieldRead) {
                return DriveUnresolvedReason.ACCESS_FAILURE;
            }
            if (safetyRejectedCallbackShape) {
                return DriveUnresolvedReason.SAFETY_REJECT;
            }
            if (callbackAccessorPresent) {
                return DriveUnresolvedReason.NO_CAPTURED_DRIVE;
            }
            if (directAccessorPresent && (sawDirectNonDriveValue || sawNonDriveBlockEntity || anyFieldRead)) {
                return DriveUnresolvedReason.DIRECT_OWNER_NOT_DRIVE;
            }
            if (accessFailure) {
                return DriveUnresolvedReason.ACCESS_FAILURE;
            }
            return DriveUnresolvedReason.OTHER;
        }
    }

    public static final class DriveCoverageSnapshot {
        public final int registeredDrives;
        public final int resolvedWatchers;
        public final int unresolvedWatchers;
        public final int uniqueResolvedDrives;
        public final long operationBegins;
        public final long resolvedOperationBegins;
        public final long unresolvedOperationBegins;
        public final long nestedSameDriveBegins;
        public final long timedOperationBegins;
        public final long beginExtract;
        public final long beginInsert;
        public final long beginPreferred;
        public final long beginAvailableStacks;
        public final long beginOther;
        public final long resolutionAttempts;
        public final long resolvedDirect;
        public final long resolvedCallback;
        public final long resolvedHostMapping;
        public final long hostMappingScans;
        public final long hostMappingWatchersSeen;
        public final long hostMappingAccessFailures;
        public final long cellResolutionAttempts;
        public final long resolvedCellSaveProvider;
        public final long cellDelegateBasic;
        public final long cellDelegateOther;
        public final long cellDelegateNull;
        public final long cellDelegateAccessFailures;
        public final long cellSaveProviderAccessFailures;
        public final long cellSaveProviderCaptureMisses;
        public final Map<String, Long> unresolvedDelegateTypes;
        public final long unresolvedNoOwnerAccessors;
        public final long unresolvedNoCapturedDrive;
        public final long unresolvedSafetyReject;
        public final long unresolvedAccessFailure;
        public final long unresolvedDirectOwnerNotDrive;
        public final long unresolvedOther;
        public final long unresolvedWithoutDirectAccessors;
        public final long unresolvedWithoutCallbackAccessors;

        private DriveCoverageSnapshot(
                int registeredDrives, int resolvedWatchers, int unresolvedWatchers, int uniqueResolvedDrives,
                long operationBegins, long resolvedOperationBegins, long unresolvedOperationBegins,
                long nestedSameDriveBegins, long timedOperationBegins,
                long beginExtract, long beginInsert, long beginPreferred, long beginAvailableStacks, long beginOther,
                long resolutionAttempts, long resolvedDirect, long resolvedCallback,
                long resolvedHostMapping, long hostMappingScans, long hostMappingWatchersSeen,
                long hostMappingAccessFailures,
                long cellResolutionAttempts, long resolvedCellSaveProvider, long cellDelegateBasic,
                long cellDelegateOther, long cellDelegateNull, long cellDelegateAccessFailures,
                long cellSaveProviderAccessFailures, long cellSaveProviderCaptureMisses,
                Map<String, Long> unresolvedDelegateTypes,
                long unresolvedNoOwnerAccessors, long unresolvedNoCapturedDrive, long unresolvedSafetyReject,
                long unresolvedAccessFailure, long unresolvedDirectOwnerNotDrive, long unresolvedOther,
                long unresolvedWithoutDirectAccessors, long unresolvedWithoutCallbackAccessors) {
            this.registeredDrives = registeredDrives;
            this.resolvedWatchers = resolvedWatchers;
            this.unresolvedWatchers = unresolvedWatchers;
            this.uniqueResolvedDrives = uniqueResolvedDrives;
            this.operationBegins = operationBegins;
            this.resolvedOperationBegins = resolvedOperationBegins;
            this.unresolvedOperationBegins = unresolvedOperationBegins;
            this.nestedSameDriveBegins = nestedSameDriveBegins;
            this.timedOperationBegins = timedOperationBegins;
            this.beginExtract = beginExtract;
            this.beginInsert = beginInsert;
            this.beginPreferred = beginPreferred;
            this.beginAvailableStacks = beginAvailableStacks;
            this.beginOther = beginOther;
            this.resolutionAttempts = resolutionAttempts;
            this.resolvedDirect = resolvedDirect;
            this.resolvedCallback = resolvedCallback;
            this.resolvedHostMapping = resolvedHostMapping;
            this.hostMappingScans = hostMappingScans;
            this.hostMappingWatchersSeen = hostMappingWatchersSeen;
            this.hostMappingAccessFailures = hostMappingAccessFailures;
            this.cellResolutionAttempts = cellResolutionAttempts;
            this.resolvedCellSaveProvider = resolvedCellSaveProvider;
            this.cellDelegateBasic = cellDelegateBasic;
            this.cellDelegateOther = cellDelegateOther;
            this.cellDelegateNull = cellDelegateNull;
            this.cellDelegateAccessFailures = cellDelegateAccessFailures;
            this.cellSaveProviderAccessFailures = cellSaveProviderAccessFailures;
            this.cellSaveProviderCaptureMisses = cellSaveProviderCaptureMisses;
            this.unresolvedDelegateTypes = Collections.unmodifiableMap(new LinkedHashMap<>(unresolvedDelegateTypes));
            this.unresolvedNoOwnerAccessors = unresolvedNoOwnerAccessors;
            this.unresolvedNoCapturedDrive = unresolvedNoCapturedDrive;
            this.unresolvedSafetyReject = unresolvedSafetyReject;
            this.unresolvedAccessFailure = unresolvedAccessFailure;
            this.unresolvedDirectOwnerNotDrive = unresolvedDirectOwnerNotDrive;
            this.unresolvedOther = unresolvedOther;
            this.unresolvedWithoutDirectAccessors = unresolvedWithoutDirectAccessors;
            this.unresolvedWithoutCallbackAccessors = unresolvedWithoutCallbackAccessors;
        }
    }

    private static final class DriveOperationFrame {
        private static final DriveOperationFrame EMPTY = new DriveOperationFrame(null, null);

        private final ResolvedTarget target;
        private final Token token;

        private DriveOperationFrame(ResolvedTarget target, Token token) {
            this.target = target;
            this.token = token;
        }
    }

    private static final class ResolvedTarget {
        private final BlockEntity blockEntity;
        private final String name;

        private ResolvedTarget(BlockEntity blockEntity, String name) {
            this.blockEntity = blockEntity;
            this.name = name;
        }
    }

    private static final class Token {
        private final Profiler.TimingData data;
        private final BlockEntity blockEntity;
        private final boolean captureAE2Spike;
        private final String spikeOperation;
        private final long startedAt;
        private final boolean tagged;
        private final Profiler.TimingData previousTarget;

        private Token(
                Profiler.TimingData data,
                BlockEntity blockEntity,
                boolean captureAE2Spike,
                String spikeOperation,
                long startedAt,
                boolean tagged,
                Profiler.TimingData previousTarget
        ) {
            this.data = data;
            this.blockEntity = blockEntity;
            this.captureAE2Spike = captureAE2Spike;
            this.spikeOperation = spikeOperation;
            this.startedAt = startedAt;
            this.tagged = tagged;
            this.previousTarget = previousTarget;
        }
    }
}
