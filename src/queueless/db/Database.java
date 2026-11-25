package queueless.db;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import queueless.exception.QueueLessException;
import queueless.util.AppLogger;

/**
 * Everything JDBC in one place:
 * <ul>
 *   <li>settings read from db.properties with byte streams (FileInputStream / FileOutputStream),</li>
 *   <li>driver loading, URL and {@link Connection} from DriverManager,</li>
 *   <li>table creation with Statement,</li>
 *   <li>{@link #execute} - runs a lambda with a Connection, closes it and converts SQLException.</li>
 * </ul>
 * If the driver or database is missing the application keeps running on in-memory collections
 * ({@link #isActive()} is false).
 */
public final class Database {

    /** One global lock keeps embedded databases (SQLite) happy under multi-threaded access. */
    public static final Object LOCK = new Object();

    private static final String[] TABLES = {"customers", "tokens", "service_history"};

    private static final String[] DDL = {
        "CREATE TABLE IF NOT EXISTS customers ("
            + "id INTEGER PRIMARY KEY, name VARCHAR(80) NOT NULL, phone VARCHAR(15) NOT NULL UNIQUE, "
            + "age INTEGER NOT NULL, priority VARCHAR(20) NOT NULL, registered_at VARCHAR(30) NOT NULL)",
        "CREATE TABLE IF NOT EXISTS tokens ("
            + "id INTEGER PRIMARY KEY, token_number VARCHAR(20) NOT NULL, customer_id INTEGER NOT NULL, "
            + "service_type VARCHAR(30) NOT NULL, priority VARCHAR(20) NOT NULL, status VARCHAR(20) NOT NULL, "
            + "issued_at VARCHAR(30) NOT NULL, counter_id INTEGER, called_at VARCHAR(30))",
        "CREATE TABLE IF NOT EXISTS service_history ("
            + "id INTEGER PRIMARY KEY, token_number VARCHAR(20) NOT NULL, customer_id INTEGER NOT NULL, "
            + "customer_name VARCHAR(80) NOT NULL, service_type VARCHAR(30) NOT NULL, counter_name VARCHAR(40) NOT NULL, "
            + "issued_at VARCHAR(30) NOT NULL, started_at VARCHAR(30) NOT NULL, completed_at VARCHAR(30) NOT NULL)"
    };

    // settings (defaults = SQLite)
    private static boolean enabled = true;
    private static String driver = "org.sqlite.JDBC";
    private static String url = "jdbc:sqlite:queueless.db";
    private static String user = "";
    private static String password = "";

    private static boolean active;
    private static String statusMessage = "Storage: not initialised";

    private Database() { }

    // ------------------------------------------------------------------ start-up

    /** Reads the settings file, then tries to connect and create the tables. Never fails because of the DB itself. */
    public static void init(String settingsPath) throws QueueLessException {
        loadSettings(settingsPath);
        active = false;
        if (!enabled) {
            statusMessage = "Storage: in-memory (database disabled in db.properties)";
        } else {
            try {
                connectOnce();
                createTables();
                active = true;
                statusMessage = "Storage: JDBC database connected (" + url + ")";
            } catch (QueueLessException e) {
                statusMessage = "Storage: in-memory (database unavailable: " + e.getMessage() + ")";
                AppLogger.error(statusMessage);
            }
        }
        AppLogger.info(statusMessage);
    }

    public static boolean isActive() { return active; }

    public static String statusMessage() { return statusMessage; }

    // ------------------------------------------------------------------ settings (byte streams)

    private static void loadSettings(String path) throws QueueLessException {
        File file = new File(path);
        if (!file.exists()) {
            storeSettings(path);
            return;
        }
        Properties p = new Properties();
        try (InputStream in = new BufferedInputStream(new FileInputStream(file))) {
            p.load(in);
        } catch (IOException e) {
            throw new QueueLessException("Cannot read " + path + ": " + e.getMessage(), e);
        }
        enabled = Boolean.parseBoolean(p.getProperty("db.enabled", "true"));
        driver = p.getProperty("db.driver", driver).trim();
        url = p.getProperty("db.url", url).trim();
        user = p.getProperty("db.user", "").trim();
        password = p.getProperty("db.password", "").trim();
    }

    private static void storeSettings(String path) throws QueueLessException {
        Properties p = new Properties();
        p.setProperty("db.enabled", String.valueOf(enabled));
        p.setProperty("db.driver", driver);
        p.setProperty("db.url", url);
        p.setProperty("db.user", user);
        p.setProperty("db.password", password);
        try (OutputStream out = new FileOutputStream(path)) {
            p.store(out, "QueueLess database settings (SQLite default, see README for MySQL)");
        } catch (IOException e) {
            throw new QueueLessException("Cannot write " + path + ": " + e.getMessage(), e);
        }
    }

    // ------------------------------------------------------------------ connections

    @SuppressWarnings("try")
    private static void connectOnce() throws QueueLessException {
        if (!driver.isEmpty()) {
            try {
                Class.forName(driver);
            } catch (ClassNotFoundException e) {
                throw QueueLessException.database("JDBC driver " + driver
                        + " not found. Put the driver jar in the lib/ folder.", e);
            }
        }
        try (Connection ignored = getConnection()) {
            // connection test only
        } catch (SQLException e) {
            throw QueueLessException.database("Cannot connect to " + url + ": " + e.getMessage(), e);
        }
    }

    public static Connection getConnection() throws QueueLessException {
        try {
            return DriverManager.getConnection(url, user, password);
        } catch (SQLException e) {
            throw QueueLessException.database("Cannot open connection: " + e.getMessage(), e);
        }
    }

    /**
     * Opens a Connection, runs the lambda, closes everything (try-with-resources) and converts
     * SQLException to a database QueueLessException.
     */
    public static <R> R execute(SqlFunction<Connection, R> work) throws QueueLessException {
        synchronized (LOCK) {
            try (Connection con = getConnection()) {
                return work.apply(con);
            } catch (SQLException e) {
                throw QueueLessException.database("Database operation failed: " + e.getMessage(), e);
            }
        }
    }

    // ------------------------------------------------------------------ schema (plain Statement)

    private static void createTables() throws QueueLessException {
        synchronized (LOCK) {
            try (Connection con = getConnection(); Statement st = con.createStatement()) {
                for (String sql : DDL) st.executeUpdate(sql);
            } catch (SQLException e) {
                throw QueueLessException.database("Could not create tables: " + e.getMessage(), e);
            }
        }
    }

    public static Map<String, Integer> tableRowCounts() throws QueueLessException {
        Map<String, Integer> counts = new LinkedHashMap<>();
        synchronized (LOCK) {
            try (Connection con = getConnection(); Statement st = con.createStatement()) {
                for (String table : TABLES) {
                    try (ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM " + table)) {
                        counts.put(table, rs.next() ? rs.getInt(1) : 0);
                    }
                }
            } catch (SQLException e) {
                throw QueueLessException.database("Could not read table sizes: " + e.getMessage(), e);
            }
        }
        return counts;
    }
}
