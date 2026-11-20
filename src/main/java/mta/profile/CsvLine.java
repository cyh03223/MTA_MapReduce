package mta.profile;

import java.util.ArrayList;
import java.util.List;

/** Minimal RFC-4180 single-line CSV parser (handles quoted fields with commas and "" escapes). */
public final class CsvLine {
    private CsvLine() {}

    public static List<String> parse(String line) {
        List<String> out = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        boolean inQuotes = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (inQuotes) {
                if (c == '"') {
                    if (i + 1 < line.length() && line.charAt(i + 1) == '"') {
                        sb.append('"');
                        i++;
                    } else {
                        inQuotes = false;
                    }
                } else {
                    sb.append(c);
                }
            } else if (c == '"') {
                inQuotes = true;
            } else if (c == ',') {
                out.add(sb.toString());
                sb.setLength(0);
            } else {
                sb.append(c);
            }
        }
        out.add(sb.toString());
        return out;
    }

    /** Field at {@code index}, trimmed; null if the row is too short. */
    public static String field(String line, int index) {
        List<String> f = parse(line);
        return index < f.size() ? f.get(index).trim() : null;
    }

    /** Joins fields into one CSV line, quoting any field that contains a comma, quote or newline. */
    public static String format(List<String> fields) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fields.size(); i++) {
            if (i > 0) sb.append(',');
            String f = fields.get(i);
            if (f.indexOf(',') >= 0 || f.indexOf('"') >= 0 || f.indexOf('\n') >= 0) {
                sb.append('"').append(f.replace("\"", "\"\"")).append('"');
            } else {
                sb.append(f);
            }
        }
        return sb.toString();
    }
}
