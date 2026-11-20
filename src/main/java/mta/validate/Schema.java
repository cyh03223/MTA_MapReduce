package mta.validate;

import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Expected schema of the cleaned data. Each column has a check that returns null when the value is
 * fine, or a short reason code. Reason codes: EMPTY, NAN_OR_NULL_TOKEN, BAD_TYPE, OUT_OF_RANGE, NOT_IN_SET.
 * Add / tighten a column here; the job and driver pick it up automatically.
 */
public final class Schema {
    private Schema() {}

    private static final Set<String> NULL_TOKENS = Set.of("nan", "null", "none", "n/a", "na", "#n/a", "inf", "-inf", "infinity", "-infinity");
    private static final Set<String> BOROUGHS = Set.of("MN", "QN", "BX", "BK", "SI");

    /** Common pre-check: empty or a NaN/null-like token. */
    static String basic(String v) {
        if (v == null || v.trim().isEmpty()) return "EMPTY";
        if (NULL_TOKENS.contains(v.trim().toLowerCase())) return "NAN_OR_NULL_TOKEN";
        return null;
    }

    static Function<String, String> timestamp(int minYear, int maxYear) {
        return v -> {
            String b = basic(v);
            if (b != null) return b;
            try {
                LocalDateTime t = LocalDateTime.parse(v.trim());
                return (t.getYear() < minYear || t.getYear() > maxYear) ? "OUT_OF_RANGE" : null;
            } catch (Exception e) { return "BAD_TYPE"; }
        };
    }

    static Function<String, String> integer(long min, long max) {
        return v -> {
            String b = basic(v);
            if (b != null) return b;
            try {
                long x = Long.parseLong(v.trim());
                return (x < min || x > max) ? "OUT_OF_RANGE" : null;
            } catch (NumberFormatException e) { return "BAD_TYPE"; }
        };
    }

    static Function<String, String> decimal(double min, double max) {
        return v -> {
            String b = basic(v);
            if (b != null) return b;
            try {
                double x = Double.parseDouble(v.trim());
                if (Double.isNaN(x) || Double.isInfinite(x)) return "NAN_OR_NULL_TOKEN";
                return (x < min || x > max) ? "OUT_OF_RANGE" : null;
            } catch (NumberFormatException e) { return "BAD_TYPE"; }
        };
    }

    static Function<String, String> oneOf(Set<String> allowed) {
        return v -> {
            String b = basic(v);
            if (b != null) return b;
            return allowed.contains(v.trim()) ? null : "NOT_IN_SET";
        };
    }

    static Function<String, String> text() { return Schema::basic; }

    /** Column name -> check. Order is the expected column order of the file. */
    public static final Map<String, Function<String, String>> CHECKS = new LinkedHashMap<>();
    static {
        CHECKS.put("transit_timestamp", timestamp(2020, 2024));
        CHECKS.put("station_complex_id", integer(1, 100000));
        CHECKS.put("station_complex", text());
        CHECKS.put("borough", oneOf(BOROUGHS));
        CHECKS.put("ridership", decimal(0, 1_000_000));
        CHECKS.put("transfers", decimal(0, 1_000_000));
        CHECKS.put("latitude", decimal(40.0, 41.5));    // NYC bounding box
        CHECKS.put("longitude", decimal(-75.0, -73.0));
    }
}
