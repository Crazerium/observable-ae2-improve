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
import java.lang.ref.WeakReference;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
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

    private static final ThreadLocal<Token> AE2_TOKEN = new ThreadLocal<>();
    private static final ThreadLocal<ArrayDeque<DriveOperationFrame>> AE2_DRIVE_OPERATION_STACK =
            ThreadLocal.withInitial(ArrayDeque::new);
    private static final Map<Object, Boolean> AE2_DRIVES =
            Collections.synchronizedMap(new WeakHashMap<>());
    private static final Map<Object, WeakReference<ResolvedTarget>> AE2_DRIVE_TARGET_CACHE =
            Collections.synchronizedMap(new WeakHashMap<>());
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

        Token token = begin(resolved);
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
            endAt(token, finishedAt);
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
        }
    }

    /**
     * Materializes currently loaded/known ME Drives immediately after
     * Observable clears its block timing map for a new session.
     */
    public static void onAE2CompatSessionStart() {
        List<Object> drives;
        synchronized (AE2_DRIVES) {
            drives = new ArrayList<>(AE2_DRIVES.keySet());
        }
        for (Object drive : drives) {
            if (drive instanceof BlockEntity) {
                ensurePassiveDriveEntry((BlockEntity) drive);
            }
        }
    }

    /** Begin one top-level DriveWatcher storage operation. */
    public static void beginAE2DriveOperation(Object driveWatcher) {
        ArrayDeque<DriveOperationFrame> stack = AE2_DRIVE_OPERATION_STACK.get();
        if (Props.notProcessing || driveWatcher == null) {
            stack.push(DriveOperationFrame.EMPTY);
            return;
        }

        ResolvedTarget target = resolveDriveWatcherTarget(driveWatcher);
        if (target == null || !isAE2DriveBlockEntity(target.blockEntity)) {
            stack.push(DriveOperationFrame.EMPTY);
            return;
        }

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

        Token token = nestedSameDrive ? null : begin(target);
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

    private static ResolvedTarget resolveDriveWatcherTarget(Object watcher) {
        WeakReference<ResolvedTarget> cachedRef = AE2_DRIVE_TARGET_CACHE.get(watcher);
        ResolvedTarget cached = cachedRef == null ? null : cachedRef.get();
        if (cached != null && !cached.blockEntity.isRemoved()) {
            return cached;
        }

        ResolvedTarget resolved = resolveTarget(watcher);
        if (resolved != null) {
            AE2_DRIVE_TARGET_CACHE.put(watcher, new WeakReference<>(resolved));
        }
        return resolved;
    }

    private static void runProfiled(ResolvedTarget target, Runnable runnable) {
        Token token = begin(target);
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

    private static Token begin(ResolvedTarget target) {
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

        return new Token(data, System.nanoTime(), tagged, previousTarget);
    }

    private static void end(Token token) {
        endAt(token, System.nanoTime());
    }

    private static void endAt(Token token, long finishedAt) {
        if (token == null) {
            return;
        }

        long elapsed = finishedAt - token.startedAt;
        synchronized (token.data) {
            token.data.setTime(token.data.getTime() + elapsed);
            token.data.setTicks(token.data.getTicks() + 1);
        }

        if (token.tagged) {
            Props.currentTarget.set(token.previousTarget);
        }
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
        return resolveTarget(source, visited, 0);
    }

    private static ResolvedTarget resolveTarget(Object source, Set<Object> visited, int depth) {
        if (source == null || depth > 8 || !visited.add(source)) {
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

        String partName = partName(source);

        for (String methodName : OWNER_METHODS) {
            Object child = invokeNoArg(source, methodName);
            if (child == null || child == source) {
                continue;
            }

            ResolvedTarget resolved = resolveTarget(child, visited, depth + 1);
            if (resolved != null) {
                if (partName != null) {
                    return new ResolvedTarget(resolved.blockEntity, partName);
                }
                return resolved;
            }
        }

        // DriveWatcher is AE2's per-cell wrapper mounted by ME Drives. Its
        // owner field name has changed across AE2 generations, so after trying
        // the common owner names above, inspect its instance fields only. This
        // keeps the reflection scope narrow while robustly resolving the
        // IChestOrDrive/DriveBlockEntity owner.
        if (isAE2DriveWatcher(type)) {
            for (Class<?> current = type; current != null && current != Object.class; current = current.getSuperclass()) {
                for (Field field : current.getDeclaredFields()) {
                    if (Modifier.isStatic(field.getModifiers())) {
                        continue;
                    }
                    try {
                        field.setAccessible(true);
                        Object child = field.get(source);
                        if (child == null || child == source) {
                            continue;
                        }
                        ResolvedTarget resolved = resolveTarget(child, visited, depth + 1);
                        if (resolved != null && isAE2DriveBlockEntity(resolved.blockEntity)) {
                            return resolved;
                        }
                    } catch (IllegalAccessException | RuntimeException ignored) {
                        // Continue with the other fields.
                    }
                }
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

                ResolvedTarget resolved = resolveTarget(child, visited, depth + 1);
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
                    ResolvedTarget resolved = resolveTarget(captured, visited, depth + 1);
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
        private final long startedAt;
        private final boolean tagged;
        private final Profiler.TimingData previousTarget;

        private Token(
                Profiler.TimingData data,
                long startedAt,
                boolean tagged,
                Profiler.TimingData previousTarget
        ) {
            this.data = data;
            this.startedAt = startedAt;
            this.tagged = tagged;
            this.previousTarget = previousTarget;
        }
    }
}
