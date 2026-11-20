# Sourced by the scripts that run Java/Maven. Picks a JDK that Hadoop 3.4.1 can run on.
# Hadoop 3.4.1 and older fail on Java 23+ ("UnsupportedOperationException: getSubject is not supported");
# Hadoop 3.4.3+ (used here) fixed that (HADOOP-19212). The jar is built for Java 17+, tested on 17, 21, 22 and 25.

java_major() { # $1 = path to a java binary -> major version number
  "$1" -version 2>&1 | awk -F'[".]' '/version/ { print ($2 == "1" ? $3 : $2); exit }'
}

pick_java() {
  local cand m
  for cand in "${JAVA_HOME:-}" \
              "$(/usr/libexec/java_home -v 21 2>/dev/null || true)" \
              "$(/usr/libexec/java_home -v 17 2>/dev/null || true)" \
              "$(/usr/libexec/java_home -v 22 2>/dev/null || true)" \
              "$(dirname "$(dirname "$(readlink -f "$(command -v java 2>/dev/null)" 2>/dev/null)")" 2>/dev/null || true)"; do
    [[ -n "$cand" && -x "$cand/bin/java" ]] || continue
    m="$(java_major "$cand/bin/java")"
    if [[ "$m" =~ ^[0-9]+$ ]] && (( m >= 17 && m <= 25 )); then
      export JAVA_HOME="$cand"; export PATH="$JAVA_HOME/bin:$PATH"
      [[ -n "${JAVA_VERBOSE:-}" ]] && echo "Using Java $m ($JAVA_HOME)" >&2
      return 0
    fi
  done
  echo "ERROR: no Java 17-25 found." >&2
  echo "Install JDK 17, 21 or 25 and either set JAVA_HOME to it or make it the default 'java'." >&2
  return 1
}

pick_java || exit 1
