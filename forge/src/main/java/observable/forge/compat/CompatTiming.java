package observable.forge.compat;

import net.minecraft.core.BlockPos;
import net.minecraft.core.registries.BuiltInRegistries;
import net.minecraft.nbt.CompoundTag;
import net.minecraft.resources.ResourceKey;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.world.item.Item;
import net.minecraft.world.item.ItemStack;
import net.minecraft.world.level.Level;
import net.minecraft.world.level.block.entity.BlockEntity;
import observable.Observable;
import observable.Props;
import observable.server.Profiler;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
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
    // v20.3.2.16: DriveWatcher is hot enough that even profiler bookkeeping must
    // be allocation-free. Reuse one small per-thread operation state instead of
    // allocating/removing an ArrayDeque + DriveOperationFrame for every storage call.
    private static final ThreadLocal<DriveOperationState> AE2_DRIVE_OPERATION_STATE =
            ThreadLocal.withInitial(DriveOperationState::new);
    private static final Map<Object, Boolean> AE2_DRIVES =
            Collections.synchronizedMap(new WeakHashMap<>());

    // v20.3.2.9: keep a weak-key owner record for every DriveWatcher observed at
    // construction/publication time. The value stores only a weak live host plus
    // immutable dimension/position/name metadata. This lets stale watchers remain
    // attributable after their original DriveBlockEntity unloads without retaining
    // chunks, levels or worlds.
    private static final Map<Object, DriveOwnerRecord> AE2_DRIVE_RECORDED_OWNERS =
            Collections.synchronizedMap(new WeakHashMap<>());

    // Exact construction context for the supplied AE2 15.5.0 build.
    // DriveBlockEntity.updateStateForSlot() is the only bytecode site that creates
    // DriveWatcher. The owner is exposed only for the duration of that call, then
    // the DriveWatcher constructor captures the exact target identity.
    private static final ThreadLocal<BlockEntity> AE2_DRIVE_CONSTRUCTION_OWNER = new ThreadLocal<>();

    // Hot-path cache is intentionally session-scoped and identity-based. It
    // avoids WeakHashMap cleanup/hash overhead on every DriveWatcher operation
    // and is cleared when a profile starts/ends so it cannot retain a world.
    private static final Map<Object, ResolvedTarget> AE2_DRIVE_TARGET_CACHE =
            Collections.synchronizedMap(new IdentityHashMap<>());
    private static final Set<Object> AE2_DRIVE_TARGET_MISS_CACHE =
            Collections.newSetFromMap(Collections.synchronizedMap(new IdentityHashMap<>()));
    // Physical registration/materialization is also session-scoped. A resolved
    // watcher can execute thousands of sub-microsecond calls per tick; repeating
    // registerAE2Drive/processCompatBlockEntity/registerPhysicalDevice on every
    // call costs more than the storage operation itself. Prepare each live Drive
    // only once per profiling session.
    private static final Set<BlockEntity> AE2_DRIVE_SESSION_PREPARED =
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

    // v20.3.2.9 typed stale-watcher fallback. Clean AE2 15.5.0 DriveWatcher
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

    // v20.3.2.13: stable cell-key ownership. GTO repeatedly creates/reuses
    // DriveWatcher wrappers around BasicCellInventory instances. Exact watcher
    // identity is therefore not sufficient for stale wrappers that remain in
    // NetworkStorage. Index the known delegate and its backing ItemStack with
    // weak keys, plus the GTO BasicCellInventory UUID (NBT key "u") in a
    // bounded LRU. Values are DriveOwnerRecord snapshots and never retain Level.
    private static final int MAX_DRIVE_CELL_UUID_OWNERS = 16384;
    private static final Map<Object, DriveOwnerRecord> AE2_DRIVE_RECORDED_DELEGATE_OWNERS =
            Collections.synchronizedMap(new WeakHashMap<>());
    private static final Map<ItemStack, DriveOwnerRecord> AE2_DRIVE_RECORDED_STACK_OWNERS =
            Collections.synchronizedMap(new WeakHashMap<>());
    private static final Map<UUID, DriveOwnerRecord> AE2_DRIVE_RECORDED_UUID_OWNERS =
            Collections.synchronizedMap(new LinkedHashMap<UUID, DriveOwnerRecord>(256, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<UUID, DriveOwnerRecord> eldest) {
                    return size() > MAX_DRIVE_CELL_UUID_OWNERS;
                }
            });
    private static volatile Field ae2BasicCellItemStackField;
    private static volatile boolean ae2BasicCellItemStackFieldMissing;

    // v20.3.2.15: storage-mount ownership with exact ExtendedAE TileExDrive support. StorageService.ProviderState is the
    // authoritative point where AE2 has both the IStorageProvider (the Drive)
    // and the exact MEStorage instances that are mounted into NetworkStorage.
    // Keep a weak-key history so a watcher that later disappears from invBySlot
    // can still be attributed while it remains mounted/queued elsewhere.
    private static final int MAX_DRIVE_MOUNTED_STORAGES_PER_PROVIDER = 128;
    private static final Map<Object, DriveOwnerRecord> AE2_DRIVE_RECORDED_MOUNT_OWNERS =
            Collections.synchronizedMap(new WeakHashMap<>());
    private static final Map<Object, String> AE2_DRIVE_MOUNT_PROVIDER_TYPES =
            Collections.synchronizedMap(new WeakHashMap<>());
    private static volatile Field ae2StorageProviderStateProviderField;
    private static volatile Field ae2StorageProviderStateInventoriesField;
    private static volatile boolean ae2StorageProviderStateFieldsMissing;

    private static final Map<String, Long> AE2_DRIVE_UNRESOLVED_DELEGATE_TYPES =
            Collections.synchronizedMap(new LinkedHashMap<>());

    // v20.3.2.9: session-local DriveWatcher coverage diagnostics. These are
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
    private static long ae2DriveResolvedRecordedOwner;
    private static long ae2DriveResolvedRecordedSnapshot;
    private static long ae2DriveResolvedStorageMountOwner;
    private static int ae2DriveStorageMountOwnerEntriesAtSessionStart;
    private static long ae2DriveStorageProviderMountPasses;
    private static long ae2DriveStorageMountWatchersSeen;
    private static long ae2DriveStorageMountAccessFailures;
    private static long ae2DriveConstructorOwnerCaptures;
    private static int ae2DriveRecordedOwnerEntriesAtSessionStart;
    private static long ae2DriveRecordedOwnerUpdates;
    private static long ae2DriveRecordedOwnerAccessFailures;
    private static long ae2DriveResolvedHostMapping;
    private static long ae2DriveHostMappingScans;
    private static long ae2DriveHostMappingWatchersSeen;
    private static long ae2DriveHostMappingAccessFailures;
    private static long ae2DriveCellResolutionAttempts;
    private static long ae2DriveStableCellResolutionAttempts;
    private static long ae2DriveResolvedCellDelegateIdentity;
    private static long ae2DriveResolvedCellStackIdentity;
    private static long ae2DriveResolvedCellUuid;
    private static long ae2DriveResolvedCellSaveProvider;
    private static long ae2DriveCellDelegateBasic;
    private static long ae2DriveCellDelegateOther;
    private static long ae2DriveCellDelegateNull;
    private static long ae2DriveCellDelegateAccessFailures;
    private static long ae2DriveCellSaveProviderAccessFailures;
    private static long ae2DriveCellSaveProviderCaptureMisses;
    private static long ae2DriveHotPathTimingStateInitializations;
    private static long ae2DriveSessionPhysicalPreparations;
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
        if (!Props.compatProfilerEnabled || Props.notProcessing || tickManager == null) {
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
        if (!Props.compatProfilerEnabled || Props.notProcessing) {
            AE2_TOKEN.remove();
            return;
        }
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
        if (!isAE2StorageDriveBlockEntity(blockEntity)) {
            return;
        }
        AE2_DRIVES.put(drive, Boolean.TRUE);
        if (Props.compatProfilerEnabled && !Props.notProcessing) {
            prepareAE2DriveForSession(blockEntity);
        }
    }

    /**
     * v20.3.2.16 hot-path guard: perform physical bucket registration and the
     * standard-Drive invBySlot pre-map at most once per live Drive per profile.
     * The set is identity-based and cleared at session end, so it cannot retain
     * worlds beyond the explicit profiling window.
     */
    private static void prepareAE2DriveForSession(BlockEntity blockEntity) {
        if (blockEntity == null || blockEntity.isRemoved() || !isAE2StorageDriveBlockEntity(blockEntity)) {
            return;
        }
        Level level = blockEntity.getLevel();
        if (!(level instanceof ServerLevel)) {
            return;
        }
        AE2_DRIVES.put(blockEntity, Boolean.TRUE);
        if (!AE2_DRIVE_SESSION_PREPARED.add(blockEntity)) {
            return;
        }
        ae2DriveSessionPhysicalPreparations++;
        ensurePassiveDriveEntry(blockEntity);
        // invBySlot is an exact AE2 DriveBlockEntity-only relation. ExtendedAE
        // drives are indexed from StorageService.ProviderState.mount() instead.
        if (isAE2DriveBlockEntity(blockEntity)) {
            indexDriveWatchersForSession(blockEntity);
        }
    }

    /**
     * Opens the exact owner context used while AE2 constructs a DriveWatcher in
     * DriveBlockEntity.updateStateForSlot(). v20.3.2.13 deliberately keeps this
     * tiny identity-history hook active even when no profiler is running. The
     * expensive timing/lifecycle collectors remain opt-in; this path performs
     * no nanoTime calls, grid scans or storage-graph reflection.
     */
    public static void beginAE2DriveWatcherOwnerContext(Object drive) {
        if (!(drive instanceof BlockEntity)) {
            AE2_DRIVE_CONSTRUCTION_OWNER.remove();
            return;
        }
        BlockEntity blockEntity = (BlockEntity) drive;
        if (!isAE2DriveBlockEntity(blockEntity) || blockEntity.isRemoved()) {
            AE2_DRIVE_CONSTRUCTION_OWNER.remove();
            return;
        }
        AE2_DRIVES.put(drive, Boolean.TRUE);
        AE2_DRIVE_CONSTRUCTION_OWNER.set(blockEntity);
    }

    /** Closes the short-lived DriveWatcher construction owner context. */
    public static void endAE2DriveWatcherOwnerContext(Object drive) {
        BlockEntity current = AE2_DRIVE_CONSTRUCTION_OWNER.get();
        if (current == null || current == drive) {
            AE2_DRIVE_CONSTRUCTION_OWNER.remove();
            return;
        }
        // Fail closed on unexpected nesting/mismatch rather than leaking a stale owner
        // into a later DriveWatcher construction.
        AE2_DRIVE_CONSTRUCTION_OWNER.remove();
    }

    /**
     * Called from DriveWatcher's constructor RETURN. At this exact bytecode point
     * the current owner context identifies the DriveBlockEntity that created this
     * exact watcher identity. No reflection or lambda-shape inference is involved.
     */
    public static void recordAE2DriveWatcherConstructed(Object watcher) {
        if (watcher == null || !isAE2DriveWatcher(watcher.getClass())) {
            return;
        }
        BlockEntity owner = AE2_DRIVE_CONSTRUCTION_OWNER.get();
        if (owner == null || owner.isRemoved() || !isAE2DriveBlockEntity(owner)) {
            return;
        }

        // Persist the exact identity relation for the whole server lifetime.
        // Weak keys ensure an unused watcher can still be collected; the value
        // retains no Level/chunk, only a weak live owner plus immutable location.
        rememberAE2DriveWatcherOwner(watcher, owner);

        // Session counters/caches stay strictly opt-in. Passive history capture
        // therefore adds no normal Observable profiler work.
        if (Props.compatProfilerEnabled && !Props.notProcessing) {
            ae2DriveConstructorOwnerCaptures++;
            AE2_DRIVE_TARGET_MISS_CACHE.remove(watcher);
        }
    }

    /**
     * Records the exact owner of the watcher just published by
     * DriveBlockEntity.updateStateForSlot(slot). This remains as a second exact
     * owner-side check behind the constructor capture. The stored value is a
     * weak-safe immutable owner snapshot, not a strong world reference.
     */
    public static void recordAE2DriveWatcherSlotOwner(Object drive, int slot) {
        if (!(drive instanceof BlockEntity)) {
            return;
        }
        BlockEntity blockEntity = (BlockEntity) drive;
        if (!isAE2DriveBlockEntity(blockEntity) || blockEntity.isRemoved()) {
            return;
        }

        AE2_DRIVES.put(drive, Boolean.TRUE);

        Field field = driveWatchersBySlotField(blockEntity.getClass());
        if (field == null) {
            if (Props.compatProfilerEnabled && !Props.notProcessing) {
                ae2DriveRecordedOwnerAccessFailures++;
            }
            return;
        }

        try {
            Object value = field.get(blockEntity);
            if (!(value instanceof Object[])) {
                if (Props.compatProfilerEnabled && !Props.notProcessing) {
                    ae2DriveRecordedOwnerAccessFailures++;
                }
                return;
            }
            Object[] watchers = (Object[]) value;
            if (slot < 0 || slot >= watchers.length) {
                return;
            }
            Object watcher = watchers[slot];
            if (watcher == null || !isAE2DriveWatcher(watcher.getClass())) {
                return;
            }

            rememberAE2DriveWatcherOwner(watcher, blockEntity);
            if (Props.compatProfilerEnabled && !Props.notProcessing) {
                ae2DriveRecordedOwnerUpdates++;
                // A watcher can theoretically be observed during a rebuild
                // before this RETURN hook executes. Allow it to be retried.
                AE2_DRIVE_TARGET_MISS_CACHE.remove(watcher);
            }
        } catch (Throwable ignored) {
            if (Props.compatProfilerEnabled && !Props.notProcessing) {
                ae2DriveRecordedOwnerAccessFailures++;
            }
        }
    }

    /**
     * Records the authoritative provider -> mounted storage relation after
     * StorageService.ProviderState.mount() completes. This hook is passive and
     * runs even when Observable is not profiling: provider mounting is rare,
     * bounded, and requires no timing, grid traversal or storage-graph walk.
     *
     * The supplied AE2 15.5.0 ProviderState keeps exactly two fields we need:
     * provider (IStorageProvider) and inventories (Set<MEStorage>). We inspect
     * only those direct fields. For a DriveBlockEntity provider, any mounted
     * DriveWatcher is an exact ownership relation by AE2's own mount contract.
     */
    public static void recordAE2StorageProviderMount(Object providerState) {
        if (providerState == null) {
            return;
        }

        Field[] fields = storageProviderStateFields(providerState.getClass());
        if (fields == null) {
            if (Props.compatProfilerEnabled && !Props.notProcessing) {
                ae2DriveStorageMountAccessFailures++;
            }
            return;
        }

        try {
            Object provider = fields[0].get(providerState);
            Object inventoriesValue = fields[1].get(providerState);
            if (!(inventoriesValue instanceof Iterable<?>)) {
                if (Props.compatProfilerEnabled && !Props.notProcessing) {
                    ae2DriveStorageMountAccessFailures++;
                }
                return;
            }

            String providerType = provider == null ? "<null>" : provider.getClass().getName();
            BlockEntity drive = provider instanceof BlockEntity ? (BlockEntity) provider : null;
            boolean driveProvider = isAE2StorageDriveBlockEntity(drive) && !drive.isRemoved();
            DriveOwnerRecord record = driveProvider ? DriveOwnerRecord.capture(drive) : null;
            boolean profiling = Props.compatProfilerEnabled && !Props.notProcessing;

            if (driveProvider) {
                AE2_DRIVES.put(drive, Boolean.TRUE);
                if (profiling) {
                    ae2DriveStorageProviderMountPasses++;
                    // Provider mounts are rare. Prepare the physical bucket here
                    // once so even a newly mounted but idle drive remains visible.
                    prepareAE2DriveForSession(drive);
                }
            }

            int scanned = 0;
            for (Object storage : (Iterable<?>) inventoriesValue) {
                if (scanned++ >= MAX_DRIVE_MOUNTED_STORAGES_PER_PROVIDER) {
                    break;
                }
                if (storage == null || !isAE2DriveWatcher(storage.getClass())) {
                    continue;
                }

                // Diagnostic source tagging is useful even for non-Drive providers:
                // if a future unresolved watcher was mounted by an addon, the report
                // can name that provider class without walking the storage object.
                AE2_DRIVE_MOUNT_PROVIDER_TYPES.put(storage, providerType);

                if (record != null) {
                    AE2_DRIVE_RECORDED_MOUNT_OWNERS.put(storage, record);
                    if (profiling) {
                        ae2DriveStorageMountWatchersSeen++;
                        // A watcher can become active before/while a provider refresh
                        // completes. Allow a previous first-seen miss to be retried.
                        AE2_DRIVE_TARGET_MISS_CACHE.remove(storage);
                    }
                }
            }
        } catch (Throwable ignored) {
            if (Props.compatProfilerEnabled && !Props.notProcessing) {
                ae2DriveStorageMountAccessFailures++;
            }
        }
    }

    private static Field[] storageProviderStateFields(Class<?> providerStateType) {
        if (providerStateType == null || ae2StorageProviderStateFieldsMissing
                || !"appeng.me.service.StorageService$ProviderState".equals(providerStateType.getName())) {
            return null;
        }
        Field provider = ae2StorageProviderStateProviderField;
        Field inventories = ae2StorageProviderStateInventoriesField;
        if (provider != null && inventories != null) {
            return new Field[]{provider, inventories};
        }

        synchronized (CompatTiming.class) {
            provider = ae2StorageProviderStateProviderField;
            inventories = ae2StorageProviderStateInventoriesField;
            if (provider != null && inventories != null) {
                return new Field[]{provider, inventories};
            }
            if (ae2StorageProviderStateFieldsMissing) {
                return null;
            }
            try {
                provider = providerStateType.getDeclaredField("provider");
                inventories = providerStateType.getDeclaredField("inventories");
                provider.setAccessible(true);
                inventories.setAccessible(true);
                ae2StorageProviderStateProviderField = provider;
                ae2StorageProviderStateInventoriesField = inventories;
                return new Field[]{provider, inventories};
            } catch (Throwable ignored) {
                ae2StorageProviderStateFieldsMissing = true;
                return null;
            }
        }
    }

    /**
     * Materializes currently loaded/known ME Drives immediately after
     * Observable clears its block timing map for a new session.
     */
    public static void onAE2CompatSessionStart() {
        clearDriveSessionCaches();
        resetDriveCoverageCounters();
        synchronized (AE2_DRIVE_RECORDED_OWNERS) {
            // Snapshot before live-drive pre-map refresh. A value above zero proves
            // passive constructor history was already populated before /observable ae2.
            ae2DriveRecordedOwnerEntriesAtSessionStart = AE2_DRIVE_RECORDED_OWNERS.size();
        }
        synchronized (AE2_DRIVE_RECORDED_MOUNT_OWNERS) {
            // Unlike invBySlot pre-map, this relation was captured when AE2 itself
            // mounted the storage provider, potentially long before profiling.
            ae2DriveStorageMountOwnerEntriesAtSessionStart = AE2_DRIVE_RECORDED_MOUNT_OWNERS.size();
        }

        List<Object> drives;
        synchronized (AE2_DRIVES) {
            drives = new ArrayList<>(AE2_DRIVES.keySet());
        }
        for (Object drive : drives) {
            if (drive instanceof BlockEntity) {
                prepareAE2DriveForSession((BlockEntity) drive);
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
        synchronized (AE2_DRIVE_SESSION_PREPARED) {
            AE2_DRIVE_SESSION_PREPARED.clear();
        }
        synchronized (AE2_DRIVE_UNRESOLVED_DELEGATE_TYPES) {
            AE2_DRIVE_UNRESOLVED_DELEGATE_TYPES.clear();
        }
        AE2_DRIVE_CONSTRUCTION_OWNER.remove();
        AE2_DRIVE_OPERATION_STATE.remove();
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
        ae2DriveResolvedRecordedOwner = 0L;
        ae2DriveResolvedRecordedSnapshot = 0L;
        ae2DriveResolvedStorageMountOwner = 0L;
        ae2DriveStorageProviderMountPasses = 0L;
        ae2DriveStorageMountWatchersSeen = 0L;
        ae2DriveStorageMountAccessFailures = 0L;
        ae2DriveConstructorOwnerCaptures = 0L;
        ae2DriveRecordedOwnerUpdates = 0L;
        ae2DriveRecordedOwnerAccessFailures = 0L;
        ae2DriveResolvedHostMapping = 0L;
        ae2DriveHostMappingScans = 0L;
        ae2DriveHostMappingWatchersSeen = 0L;
        ae2DriveHostMappingAccessFailures = 0L;
        ae2DriveCellResolutionAttempts = 0L;
        ae2DriveStableCellResolutionAttempts = 0L;
        ae2DriveResolvedCellDelegateIdentity = 0L;
        ae2DriveResolvedCellStackIdentity = 0L;
        ae2DriveResolvedCellUuid = 0L;
        ae2DriveResolvedCellSaveProvider = 0L;
        ae2DriveCellDelegateBasic = 0L;
        ae2DriveCellDelegateOther = 0L;
        ae2DriveCellDelegateNull = 0L;
        ae2DriveCellDelegateAccessFailures = 0L;
        ae2DriveCellSaveProviderAccessFailures = 0L;
        ae2DriveCellSaveProviderCaptureMisses = 0L;
        ae2DriveHotPathTimingStateInitializations = 0L;
        ae2DriveSessionPhysicalPreparations = 0L;
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
            Set<String> unique = new java.util.HashSet<>();
            for (ResolvedTarget target : AE2_DRIVE_TARGET_CACHE.values()) {
                if (target == null) {
                    continue;
                }
                if (target.dimension != null && target.position != null) {
                    unique.add(target.dimension.location() + "|" + target.position.asLong());
                } else if (target.blockEntity != null) {
                    unique.add("live@" + System.identityHashCode(target.blockEntity));
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

        Map<String, Long> unresolvedMountProviderTypes = new LinkedHashMap<>();
        List<Object> unresolvedIdentities;
        synchronized (AE2_DRIVE_TARGET_MISS_CACHE) {
            unresolvedIdentities = new ArrayList<>(AE2_DRIVE_TARGET_MISS_CACHE);
        }
        for (Object watcher : unresolvedIdentities) {
            String providerType = AE2_DRIVE_MOUNT_PROVIDER_TYPES.get(watcher);
            if (providerType == null) {
                continue;
            }
            if (unresolvedMountProviderTypes.containsKey(providerType)
                    || unresolvedMountProviderTypes.size() < MAX_DRIVE_DELEGATE_TYPE_DIAGNOSTICS) {
                unresolvedMountProviderTypes.merge(providerType, 1L, Long::sum);
            }
        }

        int mountProviderIdentityEntries;
        synchronized (AE2_DRIVE_MOUNT_PROVIDER_TYPES) {
            mountProviderIdentityEntries = AE2_DRIVE_MOUNT_PROVIDER_TYPES.size();
        }
        int recordedOwnerEntries;
        synchronized (AE2_DRIVE_RECORDED_OWNERS) {
            recordedOwnerEntries = AE2_DRIVE_RECORDED_OWNERS.size();
        }
        int storageMountOwnerEntries;
        synchronized (AE2_DRIVE_RECORDED_MOUNT_OWNERS) {
            storageMountOwnerEntries = AE2_DRIVE_RECORDED_MOUNT_OWNERS.size();
        }
        int cellDelegateOwnerEntries;
        synchronized (AE2_DRIVE_RECORDED_DELEGATE_OWNERS) {
            cellDelegateOwnerEntries = AE2_DRIVE_RECORDED_DELEGATE_OWNERS.size();
        }
        int cellStackOwnerEntries;
        synchronized (AE2_DRIVE_RECORDED_STACK_OWNERS) {
            cellStackOwnerEntries = AE2_DRIVE_RECORDED_STACK_OWNERS.size();
        }
        int cellUuidOwnerEntries;
        synchronized (AE2_DRIVE_RECORDED_UUID_OWNERS) {
            cellUuidOwnerEntries = AE2_DRIVE_RECORDED_UUID_OWNERS.size();
        }

        return new DriveCoverageSnapshot(
                registeredDrives, resolvedWatchers, unresolvedWatchers, uniqueResolvedDrives,
                ae2DriveOperationBegins, ae2DriveResolvedOperationBegins, ae2DriveUnresolvedOperationBegins,
                ae2DriveNestedSameDriveBegins, ae2DriveTimedOperationBegins,
                ae2DriveHotPathTimingStateInitializations, ae2DriveSessionPhysicalPreparations,
                ae2DriveBeginExtract, ae2DriveBeginInsert, ae2DriveBeginPreferred,
                ae2DriveBeginAvailableStacks, ae2DriveBeginOther,
                ae2DriveResolutionAttempts, ae2DriveResolvedDirect, ae2DriveResolvedCallback,
                ae2DriveResolvedRecordedOwner, ae2DriveResolvedRecordedSnapshot,
                ae2DriveResolvedStorageMountOwner, storageMountOwnerEntries, ae2DriveStorageMountOwnerEntriesAtSessionStart,
                ae2DriveStorageProviderMountPasses, ae2DriveStorageMountWatchersSeen, ae2DriveStorageMountAccessFailures,
                mountProviderIdentityEntries, unresolvedMountProviderTypes,
                recordedOwnerEntries, ae2DriveRecordedOwnerEntriesAtSessionStart, ae2DriveConstructorOwnerCaptures, ae2DriveRecordedOwnerUpdates,
                ae2DriveRecordedOwnerAccessFailures,
                ae2DriveResolvedHostMapping, ae2DriveHostMappingScans,
                ae2DriveHostMappingWatchersSeen, ae2DriveHostMappingAccessFailures,
                ae2DriveCellResolutionAttempts, ae2DriveStableCellResolutionAttempts,
                ae2DriveResolvedCellDelegateIdentity, ae2DriveResolvedCellStackIdentity, ae2DriveResolvedCellUuid,
                cellDelegateOwnerEntries, cellStackOwnerEntries, cellUuidOwnerEntries,
                ae2DriveResolvedCellSaveProvider,
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

    /**
     * Begin one named DriveWatcher storage operation. v20.3.2.16 keeps this path
     * allocation-free after first-seen target preparation: the per-thread state,
     * TimingData bucket and physical spike handle are all reused for the session.
     */
    public static void beginAE2DriveOperation(Object driveWatcher, String operation) {
        if (!Props.compatProfilerEnabled || Props.notProcessing) {
            return;
        }
        DriveOperationState state = AE2_DRIVE_OPERATION_STATE.get();
        if (driveWatcher == null) {
            state.push(null, null, null, null, 0L, false, null);
            return;
        }

        countDriveOperationBegin(operation);
        ResolvedTarget target = resolveDriveWatcherTarget(driveWatcher);
        if (!isResolvedAE2DriveTarget(target)) {
            ae2DriveUnresolvedOperationBegins++;
            state.push(null, null, null, null, 0L, false, null);
            return;
        }
        ae2DriveResolvedOperationBegins++;

        // DriveWatcher methods can delegate to one another. Only measure the
        // outermost call for the same physical drive so nested insert/extract
        // calls do not double-count the same storage work.
        boolean nestedSameDrive = state.containsTarget(target);
        if (nestedSameDrive) {
            ae2DriveNestedSameDriveBegins++;
            state.push(target, null, null, null, 0L, false, null);
            return;
        }

        DriveTimingState timing = driveTimingState(target);
        if (timing == null || timing.data == null) {
            state.push(target, null, null, null, 0L, false, null);
            return;
        }

        Profiler profiler = Observable.INSTANCE.getPROFILER();
        boolean tagged = false;
        Profiler.TimingData previousTarget = null;
        try {
            tagged = Thread.currentThread() == profiler.getServerThread();
        } catch (Throwable ignored) {
            // serverThread is lateinit. Compat must fail soft if profiling starts unusually early.
        }
        if (tagged) {
            previousTarget = Props.currentTarget.getAndSet(timing.data);
        }

        // Timestamp after profiler bookkeeping so the measured drive duration
        // remains the wrapped AE2 call, not the profiler's own preparation work.
        long startedAt = System.nanoTime();
        state.push(target, timing.data, timing.spikeHandle, operation, startedAt, tagged, previousTarget);
        ae2DriveTimedOperationBegins++;
    }

    /** Finish the DriveWatcher operation started by beginAE2DriveOperation. */
    public static void endAE2DriveOperation() {
        if (!Props.compatProfilerEnabled || Props.notProcessing) {
            DriveOperationState stale = AE2_DRIVE_OPERATION_STATE.get();
            stale.discardAndRestoreTargets();
            AE2_DRIVE_OPERATION_STATE.remove();
            return;
        }

        // Capture RETURN boundary before any profiler bookkeeping, matching the
        // existing generic Token path while avoiding a per-call Token allocation.
        long finishedAt = System.nanoTime();
        DriveOperationState state = AE2_DRIVE_OPERATION_STATE.get();
        if (state.depth <= 0) {
            return;
        }

        int slot = --state.depth;
        Profiler.TimingData data = state.data[slot];
        ResolvedTarget target = state.targets[slot];
        Object spikeHandle = state.spikeHandles[slot];
        String operation = state.operations[slot];
        long startedAt = state.startedAt[slot];
        boolean tagged = state.tagged[slot];
        Profiler.TimingData previousTarget = state.previousTargets[slot];
        state.clearSlot(slot);

        if (data == null || startedAt <= 0L) {
            return;
        }

        long elapsed = finishedAt - startedAt;
        synchronized (data) {
            data.setTime(data.getTime() + elapsed);
            data.setTicks(data.getTicks() + 1);
        }

        if (spikeHandle != null) {
            AE2GridProfiler.recordPhysicalSpikeHandle(spikeHandle, elapsed, operation);
        } else if (target != null && target.blockEntity != null) {
            AE2GridProfiler.recordPhysicalSpike(target.blockEntity, elapsed, operation);
        } else if (target != null) {
            AE2GridProfiler.recordPhysicalSpike(target.dimension, target.position, elapsed, operation);
        }

        if (tagged) {
            Props.currentTarget.set(previousTarget);
        }
    }

    /** Lazily materializes the exact timing bucket + spike handle once per resolved watcher target. */
    private static DriveTimingState driveTimingState(ResolvedTarget target) {
        if (target == null || Props.notProcessing) {
            return null;
        }
        DriveTimingState cached = target.driveTimingState;
        if (cached != null) {
            return cached;
        }

        synchronized (target) {
            cached = target.driveTimingState;
            if (cached != null) {
                return cached;
            }

            Profiler profiler = Observable.INSTANCE.getPROFILER();
            Profiler.TimingData data;
            Object spikeHandle;
            if (target.blockEntity != null) {
                BlockEntity blockEntity = target.blockEntity;
                if (blockEntity.isRemoved()) {
                    return null;
                }
                Level level = blockEntity.getLevel();
                if (!(level instanceof ServerLevel)) {
                    return null;
                }
                prepareAE2DriveForSession(blockEntity);
                data = profiler.processCompatBlockEntity(blockEntity, level, target.name);
                spikeHandle = AE2GridProfiler.physicalSpikeHandle(blockEntity);
            } else {
                if (!target.virtualDrive || target.dimension == null || target.position == null) {
                    return null;
                }
                AE2GridProfiler.registerVirtualPhysicalDevice(target.dimension, target.position, target.name);
                data = profiler.processCompatVirtualBlock(target.dimension, target.position, target.name);
                spikeHandle = AE2GridProfiler.physicalSpikeHandle(target.dimension, target.position);
            }

            cached = new DriveTimingState(data, spikeHandle);
            target.driveTimingState = cached;
            ae2DriveHotPathTimingStateInitializations++;
            return cached;
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

    /** ExtendedAE 1.20.x drive provider used by the user's 1.4.15 build. */
    private static boolean isExtendedAEDriveBlockEntity(BlockEntity blockEntity) {
        return blockEntity != null
                && "com.glodblock.github.extendedae.common.tileentities.TileExDrive"
                .equals(blockEntity.getClass().getName());
    }

    /**
     * Storage providers whose mounted DriveWatcher operations should be attributed
     * as physical ME drives. Keep this exact-class whitelist deliberately narrow:
     * constructor/invBySlot logic remains AE2-only, while addon drives enter only
     * through AE2's authoritative provider -> mounted-storage relation.
     */
    private static boolean isAE2StorageDriveBlockEntity(BlockEntity blockEntity) {
        return isAE2DriveBlockEntity(blockEntity) || isExtendedAEDriveBlockEntity(blockEntity);
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
                rememberAE2DriveWatcherOwner(watcher, blockEntity);
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
            if (cached.isUsable()) {
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

        // v20.3.2.9: prefer the exact owner-side relation captured when AE2
        // published this watcher. This is a single weak-map lookup on the first
        // operation for an identity, then the normal session cache takes over.
        ResolvedTarget resolved = resolveDriveWatcherRecordedOwner(watcher);
        if (resolved != null) {
            ae2DriveResolvedRecordedOwner++;
            if (resolved.virtualDrive) {
                ae2DriveResolvedRecordedSnapshot++;
            }
            AE2_DRIVE_TARGET_CACHE.put(watcher, resolved);
            return resolved;
        }

        // v20.3.2.15: authoritative AE2 storage mount relation (AE2 Drive + ExtendedAE TileExDrive). This catches
        // DriveWatcher instances that are still mounted/queued in NetworkStorage
        // even when they are no longer present in the Drive's current invBySlot.
        resolved = resolveDriveWatcherStorageMountOwner(watcher);
        if (resolved != null) {
            ae2DriveResolvedStorageMountOwner++;
            AE2_DRIVE_TARGET_CACHE.put(watcher, resolved);
            return resolved;
        }

        DriveResolutionProbe probe = new DriveResolutionProbe();
        resolved = resolveDriveWatcherDirectOwner(watcher, probe);
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

        // v20.3.2.13: stale wrapper identity can differ while the underlying
        // cell is still the same physical drive slot. Try only stable keys
        // recorded from exact owner-side publication: delegate identity, the
        // BasicCellInventory backing ItemStack identity, then GTO cell UUID.
        resolved = resolveDriveWatcherStableCellOwner(watcher);
        if (resolved != null) {
            AE2_DRIVE_TARGET_CACHE.put(watcher, resolved);
            return resolved;
        }

        // Legacy typed fallback: inspect only the exact AE2 15.5.0 chain
        // DriveWatcher -> BasicCellInventory.container -> direct lambda capture.
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

    private static void rememberAE2DriveWatcherOwner(Object watcher, BlockEntity blockEntity) {
        if (watcher == null || blockEntity == null) {
            return;
        }
        DriveOwnerRecord record = DriveOwnerRecord.capture(blockEntity);
        if (record != null) {
            AE2_DRIVE_RECORDED_OWNERS.put(watcher, record);
            rememberAE2DriveCellOwnerKeys(watcher, record);
        }
    }

    private static void rememberAE2DriveCellOwnerKeys(Object watcher, DriveOwnerRecord record) {
        Field delegateField = driveWatcherDelegateField(watcher.getClass());
        if (delegateField == null) {
            return;
        }
        try {
            Object delegate = delegateField.get(watcher);
            if (delegate == null) {
                return;
            }
            AE2_DRIVE_RECORDED_DELEGATE_OWNERS.put(delegate, record);
            if (!"appeng.me.cells.BasicCellInventory".equals(delegate.getClass().getName())) {
                return;
            }
            ItemStack stack = basicCellItemStack(delegate);
            if (stack == null || stack.isEmpty()) {
                return;
            }
            AE2_DRIVE_RECORDED_STACK_OWNERS.put(stack, record);
            UUID uuid = gtoCellUuid(stack);
            if (uuid != null) {
                AE2_DRIVE_RECORDED_UUID_OWNERS.put(uuid, record);
            }
        } catch (Throwable ignored) {
            // Passive capture must remain fail-soft and never affect AE2.
        }
    }

    private static ResolvedTarget resolveDriveWatcherRecordedOwner(Object watcher) {
        return resolvedTargetFromDriveOwnerRecord(AE2_DRIVE_RECORDED_OWNERS.get(watcher));
    }

    private static ResolvedTarget resolveDriveWatcherStorageMountOwner(Object watcher) {
        return resolvedTargetFromDriveOwnerRecord(AE2_DRIVE_RECORDED_MOUNT_OWNERS.get(watcher));
    }

    private static ResolvedTarget resolvedTargetFromDriveOwnerRecord(DriveOwnerRecord record) {
        if (record == null) {
            return null;
        }

        BlockEntity blockEntity = record.liveOwner.get();
        if (blockEntity != null && isAE2StorageDriveBlockEntity(blockEntity) && !blockEntity.isRemoved()) {
            return new ResolvedTarget(blockEntity, record.name);
        }

        // Stale DriveWatcher identities can remain mounted after their original
        // DriveBlockEntity unloads. Preserve only immutable coordinate metadata
        // so they can still be timed without retaining the old world/chunk.
        if (record.dimension != null && record.position != null) {
            return ResolvedTarget.virtualDrive(record.dimension, record.position, record.name);
        }
        return null;
    }

    private static ResolvedTarget resolveDriveWatcherStableCellOwner(Object watcher) {
        ae2DriveStableCellResolutionAttempts++;
        Field delegateField = driveWatcherDelegateField(watcher.getClass());
        if (delegateField == null) {
            return null;
        }

        final Object delegate;
        try {
            delegate = delegateField.get(watcher);
        } catch (Throwable ignored) {
            return null;
        }
        if (delegate == null) {
            return null;
        }

        ResolvedTarget target = resolvedTargetFromDriveOwnerRecord(
                AE2_DRIVE_RECORDED_DELEGATE_OWNERS.get(delegate));
        if (target != null) {
            ae2DriveResolvedCellDelegateIdentity++;
            return target;
        }

        if (!"appeng.me.cells.BasicCellInventory".equals(delegate.getClass().getName())) {
            return null;
        }
        ItemStack stack = basicCellItemStack(delegate);
        if (stack == null || stack.isEmpty()) {
            return null;
        }

        target = resolvedTargetFromDriveOwnerRecord(AE2_DRIVE_RECORDED_STACK_OWNERS.get(stack));
        if (target != null) {
            ae2DriveResolvedCellStackIdentity++;
            return target;
        }

        UUID uuid = gtoCellUuid(stack);
        if (uuid == null) {
            return null;
        }
        target = resolvedTargetFromDriveOwnerRecord(AE2_DRIVE_RECORDED_UUID_OWNERS.get(uuid));
        if (target != null) {
            ae2DriveResolvedCellUuid++;
        }
        return target;
    }

    private static ItemStack basicCellItemStack(Object delegate) {
        if (delegate == null || ae2BasicCellItemStackFieldMissing
                || !"appeng.me.cells.BasicCellInventory".equals(delegate.getClass().getName())) {
            return null;
        }
        Field cached = ae2BasicCellItemStackField;
        if (cached == null) {
            synchronized (CompatTiming.class) {
                cached = ae2BasicCellItemStackField;
                if (cached == null && !ae2BasicCellItemStackFieldMissing) {
                    try {
                        Field field = delegate.getClass().getDeclaredField("i");
                        if (!ItemStack.class.isAssignableFrom(field.getType())) {
                            ae2BasicCellItemStackFieldMissing = true;
                            return null;
                        }
                        field.setAccessible(true);
                        ae2BasicCellItemStackField = field;
                        cached = field;
                    } catch (Throwable ignored) {
                        ae2BasicCellItemStackFieldMissing = true;
                        return null;
                    }
                }
            }
        }
        try {
            Object value = cached == null ? null : cached.get(delegate);
            return value instanceof ItemStack ? (ItemStack) value : null;
        } catch (Throwable ignored) {
            return null;
        }
    }

    private static UUID gtoCellUuid(ItemStack stack) {
        try {
            CompoundTag tag = stack == null ? null : stack.getTag();
            if (tag == null || !tag.hasUUID("u")) {
                return null;
            }
            return tag.getUUID("u");
        } catch (Throwable ignored) {
            return null;
        }
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
        Profiler profiler = Observable.INSTANCE.getPROFILER();
        Profiler.TimingData data;

        if (blockEntity != null) {
            if (blockEntity.isRemoved()) {
                return null;
            }
            Level level = blockEntity.getLevel();
            if (!(level instanceof ServerLevel)) {
                return null;
            }
            data = profiler.processCompatBlockEntity(blockEntity, level, target.name);
        } else {
            if (!target.virtualDrive || target.dimension == null || target.position == null) {
                return null;
            }
            AE2GridProfiler.registerVirtualPhysicalDevice(target.dimension, target.position, target.name);
            data = profiler.processCompatVirtualBlock(target.dimension, target.position, target.name);
            if (target.name != null && !target.name.isBlank()) {
                data.setName(target.name);
            }
        }

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

        return new Token(
                data, blockEntity, target.dimension, target.position, captureAE2Spike,
                spikeOperation, System.nanoTime(), tagged, previousTarget);
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
            if (token.blockEntity != null) {
                AE2GridProfiler.recordPhysicalSpike(token.blockEntity, elapsed, operation);
            } else {
                AE2GridProfiler.recordPhysicalSpike(token.dimension, token.position, elapsed, operation);
            }
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

    private static boolean isResolvedAE2DriveTarget(ResolvedTarget target) {
        if (target == null) {
            return false;
        }
        if (target.blockEntity != null) {
            return isAE2StorageDriveBlockEntity(target.blockEntity) && !target.blockEntity.isRemoved();
        }
        return target.virtualDrive && target.dimension != null && target.position != null;
    }

    private static boolean samePhysicalTarget(ResolvedTarget left, ResolvedTarget right) {
        if (left == null || right == null) {
            return false;
        }
        if (left.blockEntity != null && right.blockEntity != null) {
            return left.blockEntity == right.blockEntity;
        }
        return left.dimension != null
                && left.dimension.equals(right.dimension)
                && left.position != null
                && left.position.equals(right.position);
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
            if (target == null || target.blockEntity == null || target.blockEntity.isRemoved()) {
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
        public final long hotPathTimingStateInitializations;
        public final long sessionPhysicalPreparations;
        public final long beginExtract;
        public final long beginInsert;
        public final long beginPreferred;
        public final long beginAvailableStacks;
        public final long beginOther;
        public final long resolutionAttempts;
        public final long resolvedDirect;
        public final long resolvedCallback;
        public final long resolvedRecordedOwner;
        public final long resolvedRecordedSnapshot;
        public final long resolvedStorageMountOwner;
        public final int storageMountOwnerEntries;
        public final int storageMountOwnerEntriesAtSessionStart;
        public final long storageProviderMountPasses;
        public final long storageMountWatchersSeen;
        public final long storageMountAccessFailures;
        public final int mountProviderIdentityEntries;
        public final Map<String, Long> unresolvedMountProviderTypes;
        public final int recordedOwnerEntries;
        public final int recordedOwnerEntriesAtSessionStart;
        public final long constructorOwnerCaptures;
        public final long recordedOwnerUpdates;
        public final long recordedOwnerAccessFailures;
        public final long resolvedHostMapping;
        public final long hostMappingScans;
        public final long hostMappingWatchersSeen;
        public final long hostMappingAccessFailures;
        public final long cellResolutionAttempts;
        public final long stableCellResolutionAttempts;
        public final long resolvedCellDelegateIdentity;
        public final long resolvedCellStackIdentity;
        public final long resolvedCellUuid;
        public final int cellDelegateOwnerEntries;
        public final int cellStackOwnerEntries;
        public final int cellUuidOwnerEntries;
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
                long hotPathTimingStateInitializations, long sessionPhysicalPreparations,
                long beginExtract, long beginInsert, long beginPreferred, long beginAvailableStacks, long beginOther,
                long resolutionAttempts, long resolvedDirect, long resolvedCallback,
                long resolvedRecordedOwner, long resolvedRecordedSnapshot,
                long resolvedStorageMountOwner, int storageMountOwnerEntries, int storageMountOwnerEntriesAtSessionStart,
                long storageProviderMountPasses, long storageMountWatchersSeen, long storageMountAccessFailures,
                int mountProviderIdentityEntries, Map<String, Long> unresolvedMountProviderTypes,
                int recordedOwnerEntries, int recordedOwnerEntriesAtSessionStart, long constructorOwnerCaptures, long recordedOwnerUpdates, long recordedOwnerAccessFailures,
                long resolvedHostMapping, long hostMappingScans, long hostMappingWatchersSeen,
                long hostMappingAccessFailures,
                long cellResolutionAttempts, long stableCellResolutionAttempts,
                long resolvedCellDelegateIdentity, long resolvedCellStackIdentity, long resolvedCellUuid,
                int cellDelegateOwnerEntries, int cellStackOwnerEntries, int cellUuidOwnerEntries,
                long resolvedCellSaveProvider, long cellDelegateBasic,
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
            this.hotPathTimingStateInitializations = hotPathTimingStateInitializations;
            this.sessionPhysicalPreparations = sessionPhysicalPreparations;
            this.beginExtract = beginExtract;
            this.beginInsert = beginInsert;
            this.beginPreferred = beginPreferred;
            this.beginAvailableStacks = beginAvailableStacks;
            this.beginOther = beginOther;
            this.resolutionAttempts = resolutionAttempts;
            this.resolvedDirect = resolvedDirect;
            this.resolvedCallback = resolvedCallback;
            this.resolvedRecordedOwner = resolvedRecordedOwner;
            this.resolvedRecordedSnapshot = resolvedRecordedSnapshot;
            this.resolvedStorageMountOwner = resolvedStorageMountOwner;
            this.storageMountOwnerEntries = storageMountOwnerEntries;
            this.storageMountOwnerEntriesAtSessionStart = storageMountOwnerEntriesAtSessionStart;
            this.storageProviderMountPasses = storageProviderMountPasses;
            this.storageMountWatchersSeen = storageMountWatchersSeen;
            this.storageMountAccessFailures = storageMountAccessFailures;
            this.mountProviderIdentityEntries = mountProviderIdentityEntries;
            this.unresolvedMountProviderTypes = Collections.unmodifiableMap(new LinkedHashMap<>(unresolvedMountProviderTypes));
            this.recordedOwnerEntries = recordedOwnerEntries;
            this.recordedOwnerEntriesAtSessionStart = recordedOwnerEntriesAtSessionStart;
            this.constructorOwnerCaptures = constructorOwnerCaptures;
            this.recordedOwnerUpdates = recordedOwnerUpdates;
            this.recordedOwnerAccessFailures = recordedOwnerAccessFailures;
            this.resolvedHostMapping = resolvedHostMapping;
            this.hostMappingScans = hostMappingScans;
            this.hostMappingWatchersSeen = hostMappingWatchersSeen;
            this.hostMappingAccessFailures = hostMappingAccessFailures;
            this.cellResolutionAttempts = cellResolutionAttempts;
            this.stableCellResolutionAttempts = stableCellResolutionAttempts;
            this.resolvedCellDelegateIdentity = resolvedCellDelegateIdentity;
            this.resolvedCellStackIdentity = resolvedCellStackIdentity;
            this.resolvedCellUuid = resolvedCellUuid;
            this.cellDelegateOwnerEntries = cellDelegateOwnerEntries;
            this.cellStackOwnerEntries = cellStackOwnerEntries;
            this.cellUuidOwnerEntries = cellUuidOwnerEntries;
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

    /** Cached once per resolved watcher identity for the active profile. */
    private static final class DriveTimingState {
        private final Profiler.TimingData data;
        private final Object spikeHandle;

        private DriveTimingState(Profiler.TimingData data, Object spikeHandle) {
            this.data = data;
            this.spikeHandle = spikeHandle;
        }
    }

    /**
     * Allocation-free nested DriveWatcher timing stack. Depth is normally one;
     * arrays grow only if an addon actually nests storage calls more deeply.
     */
    private static final class DriveOperationState {
        private ResolvedTarget[] targets = new ResolvedTarget[4];
        private Profiler.TimingData[] data = new Profiler.TimingData[4];
        private Object[] spikeHandles = new Object[4];
        private String[] operations = new String[4];
        private long[] startedAt = new long[4];
        private boolean[] tagged = new boolean[4];
        private Profiler.TimingData[] previousTargets = new Profiler.TimingData[4];
        private int depth;

        private boolean containsTarget(ResolvedTarget target) {
            if (target == null) {
                return false;
            }
            for (int i = 0; i < depth; i++) {
                ResolvedTarget existing = targets[i];
                if (existing != null && samePhysicalTarget(existing, target)) {
                    return true;
                }
            }
            return false;
        }

        private void push(
                ResolvedTarget target,
                Profiler.TimingData timingData,
                Object spikeHandle,
                String operation,
                long start,
                boolean isTagged,
                Profiler.TimingData previousTarget
        ) {
            ensureCapacity(depth + 1);
            int slot = depth++;
            targets[slot] = target;
            data[slot] = timingData;
            spikeHandles[slot] = spikeHandle;
            operations[slot] = operation;
            startedAt[slot] = start;
            tagged[slot] = isTagged;
            previousTargets[slot] = previousTarget;
        }

        private void clearSlot(int slot) {
            targets[slot] = null;
            data[slot] = null;
            spikeHandles[slot] = null;
            operations[slot] = null;
            startedAt[slot] = 0L;
            tagged[slot] = false;
            previousTargets[slot] = null;
        }

        private void discardAndRestoreTargets() {
            while (depth > 0) {
                int slot = --depth;
                boolean restore = tagged[slot];
                Profiler.TimingData previous = previousTargets[slot];
                clearSlot(slot);
                if (restore) {
                    Props.currentTarget.set(previous);
                }
            }
        }

        private void ensureCapacity(int required) {
            if (required <= targets.length) {
                return;
            }
            int size = Math.max(required, targets.length << 1);
            targets = java.util.Arrays.copyOf(targets, size);
            data = java.util.Arrays.copyOf(data, size);
            spikeHandles = java.util.Arrays.copyOf(spikeHandles, size);
            operations = java.util.Arrays.copyOf(operations, size);
            startedAt = java.util.Arrays.copyOf(startedAt, size);
            tagged = java.util.Arrays.copyOf(tagged, size);
            previousTargets = java.util.Arrays.copyOf(previousTargets, size);
        }
    }

    private static final class DriveOwnerRecord {
        private final WeakReference<BlockEntity> liveOwner;
        private final ResourceKey<Level> dimension;
        private final BlockPos position;
        private final String name;

        private DriveOwnerRecord(
                WeakReference<BlockEntity> liveOwner,
                ResourceKey<Level> dimension,
                BlockPos position,
                String name
        ) {
            this.liveOwner = liveOwner;
            this.dimension = dimension;
            this.position = position;
            this.name = name;
        }

        private static DriveOwnerRecord capture(BlockEntity blockEntity) {
            if (blockEntity == null || !isAE2StorageDriveBlockEntity(blockEntity)) {
                return null;
            }
            ResourceKey<Level> dimension = null;
            Level level = blockEntity.getLevel();
            if (level instanceof ServerLevel) {
                dimension = level.dimension();
            }
            BlockPos pos = blockEntity.getBlockPos();
            BlockPos position = pos == null ? null : new BlockPos(pos.getX(), pos.getY(), pos.getZ());
            return new DriveOwnerRecord(
                    new WeakReference<>(blockEntity), dimension, position, blockName(blockEntity));
        }
    }

    private static final class ResolvedTarget {
        private final BlockEntity blockEntity;
        private final ResourceKey<Level> dimension;
        private final BlockPos position;
        private final String name;
        private final boolean virtualDrive;
        private volatile DriveTimingState driveTimingState;

        private ResolvedTarget(BlockEntity blockEntity, String name) {
            this.blockEntity = blockEntity;
            Level level = blockEntity == null ? null : blockEntity.getLevel();
            this.dimension = level instanceof ServerLevel ? level.dimension() : null;
            BlockPos pos = blockEntity == null ? null : blockEntity.getBlockPos();
            this.position = pos == null ? null : new BlockPos(pos.getX(), pos.getY(), pos.getZ());
            this.name = name;
            this.virtualDrive = false;
        }

        private ResolvedTarget(ResourceKey<Level> dimension, BlockPos position, String name) {
            this.blockEntity = null;
            this.dimension = dimension;
            this.position = position == null ? null : new BlockPos(position.getX(), position.getY(), position.getZ());
            this.name = name;
            this.virtualDrive = true;
        }

        private static ResolvedTarget virtualDrive(ResourceKey<Level> dimension, BlockPos position, String name) {
            return new ResolvedTarget(dimension, position, name);
        }

        private boolean isUsable() {
            if (blockEntity != null) {
                return !blockEntity.isRemoved();
            }
            return virtualDrive && dimension != null && position != null;
        }
    }

    private static final class Token {
        private final Profiler.TimingData data;
        private final BlockEntity blockEntity;
        private final ResourceKey<Level> dimension;
        private final BlockPos position;
        private final boolean captureAE2Spike;
        private final String spikeOperation;
        private final long startedAt;
        private final boolean tagged;
        private final Profiler.TimingData previousTarget;

        private Token(
                Profiler.TimingData data,
                BlockEntity blockEntity,
                ResourceKey<Level> dimension,
                BlockPos position,
                boolean captureAE2Spike,
                String spikeOperation,
                long startedAt,
                boolean tagged,
                Profiler.TimingData previousTarget
        ) {
            this.data = data;
            this.blockEntity = blockEntity;
            this.dimension = dimension;
            this.position = position;
            this.captureAE2Spike = captureAE2Spike;
            this.spikeOperation = spikeOperation;
            this.startedAt = startedAt;
            this.tagged = tagged;
            this.previousTarget = previousTarget;
        }
    }
}
