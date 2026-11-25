package queueless.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/** Tiny file logger (character streams, append mode, exception-safe). */
public final class AppLogger {

    private static final String LOG_FILE = "logs/queueless.log";

    private AppLogger() { }

    public static synchronized void info(String message) { write("INFO ", message); }

    public static synchronized void error(String message) { write("ERROR", message); }

    private static void write(String level, String message) {
        try {
            Path parent = Paths.get(LOG_FILE).getParent();
            if (parent != null) Files.createDirectories(parent);
            try (PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(LOG_FILE, true)))) {
                pw.println(LocalDateTime.now() + " [" + level + "] [" + Thread.currentThread().getName() + "] " + message);
            }
        } catch (IOException e) {
            System.err.println("Logging failed: " + e.getMessage());
        }
    }

    /** Returns the last {@code n} log lines. */
    public static synchronized List<String> tail(int n) {
        File f = new File(LOG_FILE);
        if (!f.exists()) return new ArrayList<>();
        Deque<String> window = new ArrayDeque<>();
        try (BufferedReader br = new BufferedReader(new FileReader(f))) {
            String line;
            while ((line = br.readLine()) != null) {
                window.addLast(line);
                if (window.size() > n) window.removeFirst();
            }
        } catch (IOException e) {
            System.err.println("Cannot read log: " + e.getMessage());
        }
        return new ArrayList<>(window);
    }
}
