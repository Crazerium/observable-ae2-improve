package observable.forge.compat;

import net.minecraft.server.level.ServerLevel;
import net.minecraftforge.common.MinecraftForge;
import net.minecraftforge.event.TickEvent;
import net.minecraftforge.eventbus.api.EventPriority;
import observable.Props;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

/**
 * GT Odyssey profiler bridge which does not transform any GTCEu/GTO class.
 *
 * GTOLib performs a taint check and rejects mixins targeting protected GTCEu
 * classes such as TaskHandler and TickableSubscription. Instead, this bridge
 * runs from Forge's level-tick event before GTCEu's HIGH-priority handler and
 * replaces TaskHandler entry Runnable references at runtime.
 *
 * No GTCEu/GTO type is referenced at compile time. If GTCEu is absent or its
 * internals change, the bridge disables itself and Observable keeps running.
 */
public final class GTTaskRuntimeBridge {
    private static final Logger LOGGER = LogManager.getLogger("Observable/GTCompat");
    private static volatile Access access;
    private static volatile boolean unavailable;
    private static volatile boolean warned;

    private GTTaskRuntimeBridge() {
    }

    public static void register() {
        MinecraftForge.EVENT_BUS.addListener(EventPriority.HIGHEST, GTTaskRuntimeBridge::onLevelTick);
    }

    public static void onLevelTick(TickEvent.LevelTickEvent event) {
        if (event.phase != TickEvent.Phase.END || Props.notProcessing) {
            return;
        }
        if (!(event.level instanceof ServerLevel serverLevel)) {
            return;
        }

        Access a = getAccess(serverLevel);
        if (a == null) {
            return;
        }

        try {
            Object sync = a.getTaskHandler.invoke(serverLevel);
            wrapHandler(a, sync);

            // The async scheduler may execute concurrently. We only replace the
            // Runnable reference in an entry; queue topology is never modified.
            Object async = a.getAsyncTaskHandler.invoke(serverLevel);
            if (async != null && async != sync) {
                wrapHandler(a, async);
            }
        } catch (Throwable t) {
            warnOnce("GT runtime bridge disabled after reflective access failed", t);
            unavailable = true;
        }
    }

    private static Access getAccess(ServerLevel level) {
        if (unavailable) {
            return null;
        }
        Access current = access;
        if (current != null) {
            return current;
        }

        synchronized (GTTaskRuntimeBridge.class) {
            current = access;
            if (current != null) {
                return current;
            }
            if (unavailable) {
                return null;
            }

            try {
                ClassLoader loader = level.getClass().getClassLoader();
                Class<?> iLevel = Class.forName("com.gregtechceu.gtceu.core.ILevel", false, loader);
                if (!iLevel.isInstance(level)) {
                    unavailable = true;
                    return null;
                }

                Method getTaskHandler = iLevel.getMethod("gtceu$getTaskHandler");
                Method getAsyncTaskHandler = iLevel.getMethod("gtceu$getAsyncTaskHandler");

                Object handler = getTaskHandler.invoke(level);
                if (handler == null) {
                    return null;
                }

                Class<?> handlerClass = handler.getClass();
                while (handlerClass.getSuperclass() != null
                        && !handlerClass.getName().equals("com.gregtechceu.gtceu.utils.TaskHandler")) {
                    handlerClass = handlerClass.getSuperclass();
                }
                if (!handlerClass.getName().equals("com.gregtechceu.gtceu.utils.TaskHandler")) {
                    handlerClass = Class.forName("com.gregtechceu.gtceu.utils.TaskHandler", false, loader);
                }

                Field waitingTasks = handlerClass.getDeclaredField("waitingTasks");
                Field tasks = handlerClass.getDeclaredField("tasks");

                Class<?> tickable = Class.forName(
                        "com.gregtechceu.gtceu.api.machine.TickableSubscription",
                        false,
                        loader
                );
                Field runnable = tickable.getDeclaredField("runnable");

                Unsafe unsafe = getUnsafe();
                current = new Access(
                        getTaskHandler,
                        getAsyncTaskHandler,
                        unsafe,
                        unsafe.objectFieldOffset(waitingTasks),
                        unsafe.objectFieldOffset(tasks),
                        unsafe.objectFieldOffset(runnable)
                );
                access = current;
                LOGGER.info("Observable GT Odyssey runtime profiler bridge enabled (no GT mixins)");
                return current;
            } catch (ClassNotFoundException ignored) {
                // GTCEu is optional. Stay quiet when it is not installed.
                unavailable = true;
                return null;
            } catch (Throwable t) {
                warnOnce("Could not initialize GT Odyssey runtime profiler bridge", t);
                unavailable = true;
                return null;
            }
        }
    }

    private static void wrapHandler(Access a, Object handler) {
        if (handler == null) {
            return;
        }

        // TaskHandler synchronizes additions/merges on itself. Holding the same
        // monitor makes the sync queue scan deterministic. The async runner does
        // execute task bodies outside this monitor, but changing only a volatile
        // reference-sized field via Unsafe is safe for a best-effort profiler.
        synchronized (handler) {
            wrapQueue(a, a.unsafe.getObject(handler, a.waitingTasksOffset));
            wrapQueue(a, a.unsafe.getObject(handler, a.tasksOffset));
        }
    }

    private static void wrapQueue(Access a, Object queue) {
        if (!(queue instanceof Iterable<?> iterable)) {
            return;
        }

        try {
            for (Object entry : iterable) {
                if (entry == null) {
                    continue;
                }

                Object value = a.unsafe.getObjectVolatile(entry, a.runnableOffset);
                if (!(value instanceof Runnable runnable)) {
                    continue;
                }

                Runnable wrapped = CompatTiming.wrapGTRunnable(runnable);
                if (wrapped != runnable) {
                    a.unsafe.putObjectVolatile(entry, a.runnableOffset, wrapped);
                }
            }
        } catch (Throwable ignored) {
            // Async TaskHandler may move/unlink entries while this queue is being
            // scanned. Missing one invocation is preferable to destabilizing the
            // server; the next server tick will retry the scan.
        }
    }

    private static Unsafe getUnsafe() throws ReflectiveOperationException {
        Field field = Unsafe.class.getDeclaredField("theUnsafe");
        field.setAccessible(true);
        return (Unsafe) field.get(null);
    }

    private static void warnOnce(String message, Throwable t) {
        if (warned) {
            return;
        }
        synchronized (GTTaskRuntimeBridge.class) {
            if (warned) {
                return;
            }
            warned = true;
            LOGGER.warn(message + ": " + t);
        }
    }

    private static final class Access {
        private final Method getTaskHandler;
        private final Method getAsyncTaskHandler;
        private final Unsafe unsafe;
        private final long waitingTasksOffset;
        private final long tasksOffset;
        private final long runnableOffset;

        private Access(
                Method getTaskHandler,
                Method getAsyncTaskHandler,
                Unsafe unsafe,
                long waitingTasksOffset,
                long tasksOffset,
                long runnableOffset
        ) {
            this.getTaskHandler = getTaskHandler;
            this.getAsyncTaskHandler = getAsyncTaskHandler;
            this.unsafe = unsafe;
            this.waitingTasksOffset = waitingTasksOffset;
            this.tasksOffset = tasksOffset;
            this.runnableOffset = runnableOffset;
        }
    }
}
