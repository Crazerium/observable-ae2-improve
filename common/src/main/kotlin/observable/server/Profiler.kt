package observable.server
import dev.architectury.utils.GameInstance
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import net.minecraft.ChatFormatting
import net.minecraft.core.BlockPos
import net.minecraft.core.registries.BuiltInRegistries
import net.minecraft.network.chat.ClickEvent
import net.minecraft.network.chat.Component
import net.minecraft.network.chat.HoverEvent
import net.minecraft.resources.ResourceKey
import net.minecraft.server.level.ServerPlayer
import net.minecraft.world.entity.Entity
import net.minecraft.world.level.Level
import net.minecraft.world.level.block.entity.BlockEntity
import net.minecraft.world.level.block.entity.TickingBlockEntity
import net.minecraft.world.level.block.state.BlockState
import net.minecraft.world.level.material.FluidState
import observable.CompatProfilerReport
import observable.Observable
import observable.Props
import observable.net.S2CPacket
import java.net.HttpURLConnection
import java.net.URL
import java.nio.file.Files
import java.nio.file.Path
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.zip.GZIPOutputStream
import kotlin.concurrent.schedule
import kotlin.random.Random
private const val MAX_COMPAT_REPORT_DOWNLOAD_BYTES = 16 * 1024 * 1024

inline val StackTraceElement.classMethod
    get() = "${this.className} + ${this.methodName}"

class Profiler {
    data class TimingData(
        @Volatile var time: Long,
        @Volatile var ticks: Int,
        var traces: TraceMap,
        @Volatile var name: String = ""
    )

    var timingsMap = HashMap<Entity, TimingData>()
    lateinit var serverTraceMap: TraceMap
    lateinit var serverThread: Thread
    lateinit var samplerThread: Thread
    // TODO: consider splitting out block entity timings
    //    var blockEntityTimingsMap = HashMap<BlockEntity, TimingData>()
    var blockTimingsMap = ConcurrentHashMap<ResourceKey<Level>, ConcurrentHashMap<BlockPos, TimingData>>()
    var notProcessing
        get() = Props.notProcessing
        set(v) {
            Props.notProcessing = v
        }

    var player: ServerPlayer? = null
    var startTime: Long = 0
    var startingTicks: Int = 0
    @Volatile var lastCompletedTicks: Int = 0

    private data class CompatReportFiles(val jsonFile: Path?, val htmlFile: Path?)

    private val compatReportsByPlayer = ConcurrentHashMap<UUID, CompatReportFiles>()
    fun process(entity: Entity) =
        timingsMap.getOrPut(entity) { TimingData(0, 0, TraceMap(entity::class)) }

    fun processBlockEntity(blockEntity: TickingBlockEntity, level: Level) =
        blockTimingsMap
            .computeIfAbsent(level.dimension()) { ConcurrentHashMap() }
            .computeIfAbsent(blockEntity.pos) {
                TimingData(
                    0,
                    0,
                    TraceMap(blockEntity::class),
                    blockEntity.type
                )
            }

    fun processCompatBlockEntity(blockEntity: BlockEntity, level: Level, name: String): TimingData {
        val data = blockTimingsMap
            .computeIfAbsent(level.dimension()) { ConcurrentHashMap() }
            .computeIfAbsent(blockEntity.blockPos) {
                TimingData(
                    0,
                    0,
                    TraceMap(blockEntity::class),
                    name
                )
            }

        // AE2 parts live inside an ae2:cable_bus BlockEntity. If a vanilla
        // BlockEntity tick created the entry first, keep the same timing bucket
        // but replace the generic cable-bus name with the actual part item id.
        if (name.isNotBlank()) {
            val blockName = BuiltInRegistries.BLOCK.getKey(blockEntity.blockState.block).toString()
            if (data.name.isBlank() || data.name == blockName || data.name == blockEntity.blockState.block.descriptionId) {
                data.name = name
            }
        }

        return data
    }

    /**
     * Adds a synthetic timing point used by optional compatibility profilers.
     * The position is intentionally not required to contain a real block; the
     * client already renders block timings by dimension + BlockPos.
     */
    fun processCompatVirtualBlock(level: Level, pos: BlockPos, name: String): TimingData =
        processCompatVirtualBlock(level.dimension(), pos, name)

    /**
     * Variant for compatibility targets whose original level/chunk has already
     * unloaded but whose immutable dimension + position identity is still known.
     * No live Level reference is required to store the timing bucket.
     */
    fun processCompatVirtualBlock(dimension: ResourceKey<Level>, pos: BlockPos, name: String): TimingData {
        val data = blockTimingsMap
            .computeIfAbsent(dimension) { ConcurrentHashMap() }
            .computeIfAbsent(pos) {
                TimingData(
                    0,
                    0,
                    TraceMap(),
                    name
                )
            }

        if (name.isNotBlank() && (data.name.isBlank() || data.name.startsWith("AE2 Grid"))) {
            data.name = name
        }

        return data
    }

    fun processBlock(blockState: BlockState, pos: BlockPos, level: Level) =
        blockTimingsMap
            .computeIfAbsent(level.dimension()) { ConcurrentHashMap() }
            .computeIfAbsent(pos) {
                TimingData(
                    0,
                    0,
                    TraceMap(blockState::class),
                    blockState.block.descriptionId
                )
            }
    fun processFluid(fluidState: FluidState, pos: BlockPos, level: Level) =
        blockTimingsMap
            .computeIfAbsent(level.dimension()) { ConcurrentHashMap() }
            .computeIfAbsent(pos) {
                TimingData(
                    0,
                    0,
                    TraceMap(fluidState::class),
                    BuiltInRegistries.FLUID.getKey(fluidState.type).toString()
                )
            }
    fun startRunning(
        sample: Boolean = false,
        compatEnabled: Boolean = false,
        compatGridLimit: Int = 0
    ) {
        timingsMap.clear()
        blockTimingsMap.clear()
        serverTraceMap = TraceMap()
        startTime = System.currentTimeMillis()
        lastCompletedTicks = 0
        Props.compatProfilerEnabled = compatEnabled
        Props.compatProfilerGridLimit = if (compatEnabled) compatGridLimit.coerceAtLeast(0) else 0
        synchronized(Props.notProcessing) {
            notProcessing = false
            startingTicks = GameInstance.getServer()!!.tickCount
        }
        // Compatibility profilers are explicitly opt-in. Normal Observable
        // runs keep the AE2 collectors dormant and therefore do not scan or
        // export ME grids.
        if (compatEnabled) {
            try {
                Props.compatProfilerStartHook?.run()
            } catch (t: Throwable) {
                Observable.LOGGER.warn("Compatibility profiler start hook failed", t)
            }
        }
        if (sample) {
            samplerThread = Thread(TaggedSampler(serverThread))
            samplerThread.start()
            Thread {
                while (!Props.notProcessing) {
                    val interval = ServerSettings.traceInterval.toLong()
                    val deviation = ServerSettings.deviation.toLong()
                    serverTraceMap.add(serverThread.stackTrace.reversed().iterator())
                    Thread.sleep(interval + Random.nextLong(-deviation, deviation))
                }
            }
                .start()
        }
    }
    fun runWithDuration(
        player: ServerPlayer?,
        duration: Int,
        sample: Boolean
    ) = runWithDuration(player, duration, sample, false, 0)

    fun runWithDuration(
        player: ServerPlayer?,
        duration: Int,
        sample: Boolean,
        compatEnabled: Boolean,
        compatGridLimit: Int
    ) {
        this.player = player
        startRunning(sample, compatEnabled, compatGridLimit)
        val durMs = duration.toLong() * 1000L
        Observable.CHANNEL.sendToPlayers(
            GameInstance.getServer()!!.playerList.players,
            S2CPacket.ProfilingStarted(startTime + durMs)
        )
        Timer("Profiler", false).schedule(durMs) {
            stopRunning()
        }
    }
    private fun validCompatReportPath(path: Path?): Path? =
        path?.takeIf {
            try {
                Files.isRegularFile(it)
            } catch (_: Exception) {
                false
            }
        }

    private fun reportDownloadButton(label: String, format: String): Component =
        Component.literal(label)
            .withStyle(ChatFormatting.AQUA, ChatFormatting.UNDERLINE)
            .withStyle { style ->
                style
                    .withClickEvent(
                        ClickEvent(
                            ClickEvent.Action.RUN_COMMAND,
                            "/observable ae2 download $format"
                        )
                    )
                    .withHoverEvent(
                        HoverEvent(
                            HoverEvent.Action.SHOW_TEXT,
                            Component.literal("Передать отчёт с сервера и сохранить его локально")
                        )
                    )
            }

    private fun rememberAndOfferCompatReport(player: ServerPlayer, report: CompatProfilerReport?) {
        if (report == null) return

        val jsonFile = validCompatReportPath(report.jsonFile)
        val htmlFile = validCompatReportPath(report.htmlFile)
        if (jsonFile == null && htmlFile == null) return

        compatReportsByPlayer[player.uuid] = CompatReportFiles(jsonFile, htmlFile)
        val playerId = player.uuid
        GameInstance.getServer()?.execute {
            val online = GameInstance.getServer()?.playerList?.getPlayer(playerId) ?: return@execute
            val message = Component.literal("AE2-отчёт готов. Скачать на этот клиент:")
            if (htmlFile != null) {
                message.append(" ").append(
                    reportDownloadButton("[Скачать HTML]", "html")
                )
            }
            if (jsonFile != null) {
                message.append(" ").append(
                    reportDownloadButton("[Скачать JSON]", "json")
                )
            }
            online.sendSystemMessage(message)
        }
    }

    fun sendCompatReport(player: ServerPlayer, format: String): Boolean {
        val report = compatReportsByPlayer[player.uuid] ?: return false
        val path = when (format.lowercase(Locale.ROOT)) {
            "html" -> report.htmlFile
            "json" -> report.jsonFile
            else -> null
        } ?: return false

        return try {
            if (!Files.isRegularFile(path)) return false
            val size = Files.size(path)
            if (size <= 0L || size > MAX_COMPAT_REPORT_DOWNLOAD_BYTES) {
                Observable.LOGGER.warn(
                    "Refusing AE2 report download ${path.fileName}: ${size} bytes"
                )
                return false
            }

            val bytes = Files.readAllBytes(path)
            Observable.CHANNEL.sendToPlayersSplit(
                listOf(player),
                S2CPacket.AE2ReportFile(path.fileName.toString(), bytes)
            )
            Observable.LOGGER.info(
                "Sent AE2 report ${path.fileName} (${bytes.size} bytes) to ${player.gameProfile.name}"
            )
            true
        } catch (t: Throwable) {
            Observable.LOGGER.warn("Failed to send AE2 report $path to ${player.gameProfile.name}", t)
            false
        }
    }

    /**
     * ProfilingData stores an average rate as time / sample-count. Compatibility
     * collectors may intentionally create a zero-cost bucket (for example an
     * idle ME Drive that should still be visible in the result). A 0 / 0 bucket
     * becomes NaN and kotlinx.serialization correctly refuses to emit it as
     * JSON. Convert only unsampled buckets to one synthetic zero-cost sample so
     * their exported rate is exactly 0.0 while preserving every real sample.
     */
    private fun normalizeZeroSampleTimingsForExport(): Int {
        var normalized = 0

        fun normalize(data: TimingData) {
            if (data.ticks > 0) return
            synchronized(data) {
                if (data.ticks <= 0) {
                    // A negative time is never a valid timing sample either.
                    if (data.time < 0L) data.time = 0L
                    data.ticks = 1
                    normalized++
                }
            }
        }

        timingsMap.values.forEach(::normalize)
        blockTimingsMap.values.forEach { dimension ->
            dimension.values.forEach(::normalize)
        }
        return normalized
    }

    fun uploadProfile(data: ProfilingData, diagnostics: JsonObject): String? {
        if (ServerSettings.uploadURL.isEmpty()) {
            Observable.LOGGER.info("uploadURL not set, skipping upload")
            return null
        }

        Observable.LOGGER.info("Attempting to upload profile")
        return try {
            // Keep serialization inside the fail-soft boundary. A malformed
            // optional timing can no longer kill the Profiler timer thread and
            // prevent the compact in-game result from being delivered.
            val serialized = Json.encodeToString(DataWithDiagnostics(data, diagnostics))
            val conn = URL(ServerSettings.uploadURL).openConnection() as HttpURLConnection
            conn.requestMethod = "POST"
            conn.doOutput = true

            Observable.LOGGER.info("Writing ${String.format("%.2f", serialized.length / 1000.0)}kb")
            GZIPOutputStream(conn.outputStream).bufferedWriter(Charsets.UTF_8).use {
                it.write(serialized)
            }
            val profileURL = conn.inputStream.bufferedReader(Charsets.UTF_8).use { it.readText() }
            Observable.LOGGER.info("Profile uploaded to $profileURL")

            profileURL
        } catch (e: Exception) {
            e.printStackTrace()
            null
        }
    }
    fun stopRunning() {
        val diagnostics = getDiagnostics()
        val initiatingPlayer = player
        val ticks: Int
        synchronized(Props.notProcessing) {
            notProcessing = true
            ticks = GameInstance.getServer()!!.tickCount - startingTicks
            lastCompletedTicks = ticks
        }
        val compatEnabled = Props.compatProfilerEnabled
        // Freeze optional compatibility collectors before ProfilingData snapshots
        // the maps. Regular Observable runs skip this entire path.
        var compatReport: CompatProfilerReport? = null
        if (compatEnabled) {
            try {
                compatReport = Props.compatProfilerSnapshotHook?.get()
            } catch (t: Throwable) {
                Observable.LOGGER.warn("Compatibility profiler snapshot hook failed", t)
            } finally {
                // Detailed AE2 collection is finished. Keep only the requested
                // grid-limit value for prepareClientOverlay(); turn off hot-path
                // compat work before upload/network serialization begins.
                Props.compatProfilerEnabled = false
            }
        }
        val players = initiatingPlayer?.let { listOf(it) } ?: listOf()
        Observable.CHANNEL.sendToPlayers(players, S2CPacket.ProfilingCompleted)
        // Snapshot the normal profile plus the bounded Top-N AE2 markers (when
        // explicitly requested) for observable.tas.sh upload. Before
        // ProfilingData calculates average rates, make intentionally passive
        // zero-cost buckets JSON-safe (0 ns / 1 synthetic sample => 0.0 rate).
        // This is especially important for visible-but-idle AE2 ME Drives.
        val normalizedZeroSamples = normalizeZeroSampleTimingsForExport()
        if (normalizedZeroSamples > 0) {
            Observable.LOGGER.info(
                "Normalized $normalizedZeroSamples zero-sample timing bucket(s) for JSON-safe export"
            )
        }
        val uploadData = ProfilingData.create(timingsMap, blockTimingsMap, ticks, serverTraceMap)
        Observable.LOGGER.info("Profiler ran for $ticks ticks, sending data")
        Observable.LOGGER.info("Sending to ${players.map { it.gameProfile.name }}")
        val link = uploadProfile(uploadData, diagnostics)

        if (compatEnabled) {
            try {
                Props.compatProfilerClientViewHook?.run()
            } catch (t: Throwable) {
                Observable.LOGGER.warn("Compatibility profiler client-view hook failed", t)
            }
        }

        // Client overlay has consumed the requested Top-N limit; clear all
        // compatibility session flags before building/sending the client packet.
        Props.compatProfilerEnabled = false
        Props.compatProfilerGridLimit = 0

        val clientData = ProfilingData.create(timingsMap, blockTimingsMap, ticks, serverTraceMap)
        Observable.CHANNEL.sendToPlayersSplit(players, S2CPacket.ProfilingResult(clientData, link))
        if (compatEnabled && initiatingPlayer != null) {
            rememberAndOfferCompatReport(initiatingPlayer, compatReport)
        }
        Observable.LOGGER.info("Data transfer complete!")
        GameInstance.getServer()
            ?.playerList
            ?.players
            ?.filter { Observable.hasPermission(it) }
            ?.let { Observable.CHANNEL.sendToPlayers(it, S2CPacket.ProfilerInactive) }
    }
}
