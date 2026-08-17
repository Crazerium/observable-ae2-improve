package observable;

import java.nio.file.Path;

/**
 * Server-side files produced by an optional compatibility profiler for the
 * just-completed profiling session. Paths stay on the server; clients receive
 * file bytes only after explicitly clicking a download action.
 */
public final class CompatProfilerReport {
    private final Path jsonFile;
    private final Path htmlFile;

    public CompatProfilerReport(Path jsonFile, Path htmlFile) {
        this.jsonFile = jsonFile;
        this.htmlFile = htmlFile;
    }

    public Path getJsonFile() {
        return jsonFile;
    }

    public Path getHtmlFile() {
        return htmlFile;
    }
}
