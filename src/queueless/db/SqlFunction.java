package queueless.db;

import java.sql.SQLException;

/** Functional interface: a unit of JDBC work that may throw SQLException (used with lambdas). */
@FunctionalInterface
public interface SqlFunction<T, R> {
    R apply(T t) throws SQLException;
}
