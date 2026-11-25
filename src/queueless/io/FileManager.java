package queueless.io;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.StreamTokenizer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import queueless.exception.QueueLessException;
import queueless.model.Customer;
import queueless.model.ServiceRecord;
import queueless.util.Pair;

/**
 * All file handling in one class:
 * <ul>
 *   <li>configuration file read with a {@link StreamTokenizer},</li>
 *   <li>CSV import / export with character streams (FileReader / FileWriter),</li>
 *   <li>byte streams: raw file copy (backup) and a small binary snapshot (DataOutputStream).</li>
 * </ul>
 */
public final class FileManager {

    private static final int MAGIC = 0x514C4553;   // "QLES"

    private FileManager() { }

    // ================================================================== configuration (StreamTokenizer)

    public static Map<String, Integer> defaults() {
        Map<String, Integer> d = new LinkedHashMap<>();
        d.put("GENERAL_ENQUIRY", 2);
        d.put("PAYMENT", 2);
        d.put("DOCUMENTATION", 1);
        d.put("CONSULTATION", 1);
        d.put("QUEUE_CAPACITY", 50);
        d.put("AVG_SERVICE_MINUTES", 5);
        return d;
    }

    /**
     * Reads queueless.cfg. File format: "KEY number" pairs, '#' starts a comment. Example: PAYMENT 2
     */
    public static Map<String, Integer> loadConfig(String path) throws QueueLessException {
        File file = new File(path);
        if (!file.exists()) {
            writeDefaultConfig(file);
            return defaults();
        }
        Map<String, Integer> cfg = defaults();
        try (Reader reader = new BufferedReader(new FileReader(file))) {
            StreamTokenizer st = new StreamTokenizer(reader);
            st.commentChar('#');
            st.wordChars('_', '_');
            String key = null;
            int token;
            while ((token = st.nextToken()) != StreamTokenizer.TT_EOF) {
                if (token == StreamTokenizer.TT_WORD) {
                    key = st.sval;
                } else if (token == StreamTokenizer.TT_NUMBER) {
                    if (key == null) {
                        throw new QueueLessException("Config line " + st.lineno() + ": number without a key");
                    }
                    cfg.put(key, Math.max(1, (int) st.nval));
                    key = null;
                } else {
                    throw new QueueLessException("Config line " + st.lineno() + ": unexpected symbol");
                }
            }
        } catch (IOException e) {
            throw new QueueLessException("Cannot read " + path + ": " + e.getMessage(), e);
        }
        return cfg;
    }

    private static void writeDefaultConfig(File file) throws QueueLessException {
        try (PrintWriter pw = new PrintWriter(new FileWriter(file))) {
            pw.println("# QueueLess configuration (KEY value)");
            pw.println("# Number of counters per service");
            for (Map.Entry<String, Integer> e : defaults().entrySet()) {
                if (e.getKey().equals("QUEUE_CAPACITY")) {
                    pw.println("# Maximum tokens waiting at one time");
                } else if (e.getKey().equals("AVG_SERVICE_MINUTES")) {
                    pw.println("# Average service time used for wait estimates");
                }
                pw.println(e.getKey() + " " + e.getValue());
            }
        } catch (IOException e) {
            throw new QueueLessException("Cannot create default config: " + e.getMessage(), e);
        }
    }

    // ================================================================== CSV (character streams)

    public static void writeLines(String path, String header, List<String> lines) throws QueueLessException {
        ensureParent(path);
        try (PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(path)))) {
            pw.println(header);
            lines.forEach(pw::println);
        } catch (IOException e) {
            throw new QueueLessException("Cannot write " + path + ": " + e.getMessage(), e);
        }
    }

    public static List<String> readLines(String path) throws QueueLessException {
        List<String> lines = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = br.readLine()) != null) lines.add(line);
        } catch (IOException e) {
            throw new QueueLessException("Cannot read " + path + ": " + e.getMessage(), e);
        }
        return lines;
    }

    public static int exportCustomers(List<Customer> customers, String path) throws QueueLessException {
        writeLines(path, Customer.CSV_HEADER, customers.stream().map(Customer::toCsv).collect(Collectors.toList()));
        return customers.size();
    }

    public static int exportHistory(List<ServiceRecord> records, String path) throws QueueLessException {
        writeLines(path, ServiceRecord.CSV_HEADER, records.stream().map(ServiceRecord::toCsv).collect(Collectors.toList()));
        return records.size();
    }

    /**
     * Validating import. Returns Pair(validCustomers, errorMessages) so one bad row never stops the whole file.
     */
    public static Pair<List<Customer>, List<String>> importCustomers(String path) throws QueueLessException {
        List<String> lines = readLines(path);
        List<Customer> valid = new ArrayList<>();
        List<String> errors = new ArrayList<>();
        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i).trim();
            if (line.isEmpty() || (i == 0 && line.equalsIgnoreCase(Customer.CSV_HEADER))) continue;
            try {
                valid.add(Customer.fromCsv(line));
            } catch (QueueLessException e) {
                errors.add("Line " + (i + 1) + ": " + e.getMessage());
            }
        }
        return Pair.of(valid, errors);
    }

    // ================================================================== backup + snapshot (byte streams)

    /** Copies a file byte by byte using FileInputStream/FileOutputStream; returns bytes copied. */
    public static long copyFile(String source, String target) throws QueueLessException {
        ensureParent(target);
        long total = 0;
        byte[] buffer = new byte[4096];
        try (InputStream in = new FileInputStream(source); OutputStream out = new FileOutputStream(target)) {
            int n;
            while ((n = in.read(buffer)) != -1) {
                out.write(buffer, 0, n);
                total += n;
            }
        } catch (IOException e) {
            throw new QueueLessException("Backup failed: " + e.getMessage(), e);
        }
        return total;
    }

    /** Writes a compact binary summary (magic number, timestamp, three counters). */
    public static void writeSnapshot(String path, int customers, int tokens, int records) throws QueueLessException {
        ensureParent(path);
        try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(path)))) {
            out.writeInt(MAGIC);
            out.writeLong(System.currentTimeMillis());
            out.writeInt(customers);
            out.writeInt(tokens);
            out.writeInt(records);
        } catch (IOException e) {
            throw new QueueLessException("Cannot write snapshot: " + e.getMessage(), e);
        }
    }

    public static String readSnapshot(String path) throws QueueLessException {
        try (DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(path)))) {
            if (in.readInt() != MAGIC) throw new QueueLessException("Not a QueueLess snapshot file.");
            Instant when = Instant.ofEpochMilli(in.readLong());
            return "Snapshot taken " + when + " -> customers=" + in.readInt()
                    + ", tokens=" + in.readInt() + ", history records=" + in.readInt();
        } catch (IOException e) {
            throw new QueueLessException("Cannot read snapshot: " + e.getMessage(), e);
        }
    }

    // ================================================================== helper

    private static void ensureParent(String path) throws QueueLessException {
        File parent = new File(path).getAbsoluteFile().getParentFile();
        if (parent != null && !parent.exists() && !parent.mkdirs()) {
            throw new QueueLessException("Cannot create folder " + parent);
        }
    }
}
