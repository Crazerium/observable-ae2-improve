package observable.client

import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import net.minecraft.ChatFormatting
import net.minecraft.network.chat.ClickEvent
import net.minecraft.network.chat.Component
import observable.server.ProfilingData
import java.io.File
import java.text.SimpleDateFormat
import java.util.*

object ProfileExporter {
    private val dir = File("observable_profiles")
    private val ae2Dir = File(dir, "ae2")
    private val sdf = SimpleDateFormat("yyyy-MM-dd--HH.mm.ss")

    init {
        if (!dir.exists()) dir.mkdirs()
    }

    private val json = Json {
        prettyPrint = true
    }

    fun export(data: ProfilingData): Component {
        val file = File(dir, "${sdf.format(Date())}.json")
        file.printWriter().use {
            it.println(json.encodeToString(data))
        }

        val link = Component.literal(file.name).withStyle(ChatFormatting.UNDERLINE).withStyle {
            it.withClickEvent(ClickEvent(ClickEvent.Action.OPEN_FILE, dir.absolutePath))
        }

        return link
    }

    fun exportAE2Report(fileName: String, data: ByteArray): Component {
        if (!ae2Dir.exists() && !ae2Dir.mkdirs()) {
            throw IllegalStateException("Could not create ${ae2Dir.absolutePath}")
        }

        val safeName = File(fileName).name
        require(safeName.endsWith(".json", true) || safeName.endsWith(".html", true)) {
            "Unsupported AE2 report file: $safeName"
        }

        val file = File(ae2Dir, safeName)
        file.outputStream().use { it.write(data) }

        return Component.literal(file.name).withStyle(ChatFormatting.UNDERLINE).withStyle {
            it.withClickEvent(ClickEvent(ClickEvent.Action.OPEN_FILE, file.absolutePath))
        }
    }
}
