# snapbench

Benchmark and raw results for WT-9074. Not part of the PR.

`snapbench.c` opens one table of 100 000 keys (`key_format=Q,value_format=S`, 100-byte values) in a
1 GB cache and runs N threads in a loop: `begin_transaction`, then an update of a random key or a
search, then `commit_transaction`. It records the latency of `begin_transaction` and of the whole
transaction in histograms with buckets narrower than 1/16 of the value, for 10 s after 2 s of
warm-up, and prints one line of `key=value` pairs, including connection statistics deltas
(`stat0`..`stat9`, see `stat_names` in the source; `stat3` is `txn_id_alloc_retry`, -1 on builds
without it).

Options: `-t` threads, `-r` percentage of searches, `-n` number of threads with lowered priority
(nice 19 on Linux, `PRIO_DARWIN_BG` on macOS), `-T` timestamps (searches set a read timestamp
with `timestamp_transaction_uint` after `begin_transaction`; updates commit with a timestamp; a
thread moves stable to 10 000 behind `all_durable` and oldest to 10 000 behind stable every
millisecond), `-d`/`-W` measured and warm-up seconds, `-k` keys, `-i` idle sessions.

```sh
cc -O2 -std=c11 -I<build>/include -o snapbench-<name> snapbench.c -L<build> -lwiredtiger \
   -Wl,-rpath,<build> -lpthread
# series A: 50 % searches; C: updates only; B: 90 % searches with timestamps
./run.sh A.txt <db dir> 3 10 "4 16 64 128" "50" "0 half" 0 snapbench-base snapbench-s1000 snapbench-s100 snapbench-s0
./run.sh C.txt <db dir> 3 10 "4 16 64 128" "0" "0" 0 <binaries>
./run.sh B2.txt <db dir> 3 10 "16 64 128" "90" "0 half" 1 <binaries>
python3 summarize.py A.txt
python3 jira_tables.py A.txt ops_per_sec begin_r_p999 begin_r_max
```

Builds: `base` is develop 50c032ba6; `s1000` is this branch's parent commit (the PR); `s100` and
`s0` are the PR built with `WT_TXN_ID_ALLOC_SPINS` 100 and 0. All Release
(`-DCMAKE_BUILD_TYPE=Release -DENABLE_PYTHON=0`).

Results:

- `results/linux-x86_64`: KVM guest, AMD EPYC Genoa, 4 vCPU, 7 GB RAM, Linux 7.0, gcc 15.2;
  threads 4, 16, 64, 128.
- `results/macos-arm64`: Apple M4 Pro (10 performance + 4 efficiency cores), 24 GB RAM,
  macOS 26.5, Apple clang 21; threads 8, 14, 64, 128. A shared workstation; each line records the
  1-minute load average after the run and, in series A, C and B on this machine, the start time.
