#!/bin/sh
# Run snapbench for each build, thread count and workload, alternating build order between rounds.
#
# Usage: run.sh OUT DBDIR ROUNDS SECS "THREADS..." "READ_PCTS..." "NICE..." TS BIN...
#   OUT      result file, one snapbench line per run prefixed with "build=<name> round=<r>
#            start=<local time of the run start> load1=<1-minute load average after the run>"
#   DBDIR    scratch directory for databases (removed after each run)
#   NICE     number of low-priority worker threads; "half" means threads/2
#   TS       0 or 1: run with timestamps (-T)
#   BIN      snapbench binaries; the build name is the file name after "snapbench-"
set -u
OUT=$1 DB=$2 ROUNDS=$3 SECS=$4 THREADS=$5 READS=$6 NICES=$7 TS=$8
shift 8
mkdir -p "$DB"
r=1
while [ $r -le "$ROUNDS" ]; do
    if [ $((r % 2)) -eq 1 ]; then bins="$*"; else bins=$(for b in "$@"; do echo "$b"; done | tail -r 2>/dev/null || for b in "$@"; do echo "$b"; done | tac); fi
    for t in $THREADS; do
        for rp in $READS; do
            for nc in $NICES; do
                [ "$nc" = half ] && nc=$((t / 2))
                for bin in $bins; do
                    name=$(basename "$bin" | sed 's/^snapbench-//')
                    home=$DB/$name-$r-$t-$rp-$nc
                    rm -rf "$home" && mkdir -p "$home"
                    tsflag=""
                    [ "$TS" = 1 ] && tsflag=-T
                    start=$(date +%Y-%m-%dT%H:%M:%S)
                    line=$("$bin" -h "$home" -t "$t" -r "$rp" -n "$nc" -d "$SECS" -W 2 $tsflag 2>&1 | tail -1)
                    load=$(uptime | sed 's/.*load average[s]*: //' | cut -d' ' -f1 | tr -d ,)
                    echo "build=$name round=$r start=$start load1=$load $line" >> "$OUT"
                    rm -rf "$home"
                done
            done
        done
    done
    r=$((r + 1))
done
