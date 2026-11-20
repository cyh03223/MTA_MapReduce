package mta.clean;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/**
 * Registry of per-column cleaning rules. To clean another column, add one entry to RULES;
 * CleanDriver / CleanMapper need no changes. A rule maps one cell value to its cleaned value.
 */
public final class CleanRules {
    private CleanRules() {}

    /** Borough full name -> abbreviation. */
    static final Map<String, String> BOROUGH = Map.of(
            "Manhattan", "MN",
            "Queens", "QN",
            "Bronx", "BX",
            "Brooklyn", "BK",
            "Staten Island", "SI");

    /**
     * station_complex_id: non-numeric ids -> numeric. TRAM1/TRAM2 (Roosevelt Island Tramway) cannot just
     * drop their letters because 1 and 2 are already Astoria-Ditmars Blvd / Astoria Blvd, so they get
     * unused numbers above the current maximum (636).
     */
    static final Map<String, String> STATION_ID = Map.of(
            "TRAM1", "1001",
            "TRAM2", "1002");

    /** A cleaning rule: the transformation plus a check that the original value was one we expected. */
    public record Rule(UnaryOperator<String> apply, Predicate<String> known) {}

    /** column name (lower case) -> rule. Values that are not "known" pass through unchanged and are counted as UNMAPPED. */
    public static final Map<String, Rule> RULES = new LinkedHashMap<>();
    static {
        RULES.put("borough", new Rule(
                v -> BOROUGH.getOrDefault(v.trim(), v),
                v -> BOROUGH.containsKey(v.trim()) || BOROUGH.containsValue(v.trim())));
        RULES.put("station_complex_id", new Rule(
                v -> STATION_ID.getOrDefault(v.trim(), v),
                v -> STATION_ID.containsKey(v.trim()) || v.trim().matches("\\d+")));
    }
}
