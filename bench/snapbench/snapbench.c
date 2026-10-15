/*
 * snapbench: latency of snapshot creation (begin_transaction) and of whole transactions when
 * many threads build snapshots and allocate transaction IDs at the same time.
 *
 * Each worker loops: begin_transaction, then either an update of a random key (allocates a
 * transaction ID) or a search of a random key, then commit. With -T, writers commit with a
 * timestamp, readers set a read timestamp after begin_transaction (as mongod does for majority
 * reads) equal to the stable timestamp, and a separate thread advances the oldest and stable
 * timestamps every millisecond.
 *
 * Output: one line of key=value pairs.
 */
#define _GNU_SOURCE
#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <time.h>
#include <unistd.h>
#ifdef __linux__
#include <sys/syscall.h>
#endif

#include <wiredtiger.h>

#define HSUB_BITS 4
#define HSUB (1 << HSUB_BITS)
#define HBUCKETS (64 * HSUB)

typedef struct {
    uint64_t n;
    uint64_t max;
    uint64_t b[HBUCKETS];
} hist_t;

static unsigned
hidx(uint64_t v)
{
    unsigned msb, sub;

    if (v < HSUB)
        return ((unsigned)v);
    msb = 63 - (unsigned)__builtin_clzll(v);
    sub = (unsigned)((v >> (msb - HSUB_BITS)) & (HSUB - 1));
    return ((msb - HSUB_BITS + 1) * HSUB + sub);
}

static uint64_t
hlow(unsigned i)
{
    unsigned msb, sub;

    if (i < HSUB)
        return (i);
    msb = i / HSUB + HSUB_BITS - 1;
    sub = i % HSUB;
    return (((uint64_t)HSUB + sub) << (msb - HSUB_BITS));
}

static void
hadd(hist_t *h, uint64_t v)
{
    h->b[hidx(v)]++;
    h->n++;
    if (v > h->max)
        h->max = v;
}

static void
hmerge(hist_t *to, const hist_t *from)
{
    int i;

    for (i = 0; i < HBUCKETS; i++)
        to->b[i] += from->b[i];
    to->n += from->n;
    if (from->max > to->max)
        to->max = from->max;
}

/* Lower bound of the bucket holding quantile q, in nanoseconds. */
static uint64_t
hq(const hist_t *h, double q)
{
    uint64_t want, seen;
    int i;

    if (h->n == 0)
        return (0);
    want = (uint64_t)(q * (double)h->n);
    if (want >= h->n)
        want = h->n - 1;
    for (seen = 0, i = 0; i < HBUCKETS; i++) {
        seen += h->b[i];
        if (seen > want)
            return (hlow((unsigned)i));
    }
    return (h->max);
}

static uint64_t
now_ns(void)
{
    struct timespec ts;

    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ((uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec);
}

#define CHECK(call)                                                                       \
    do {                                                                                  \
        int __r = (call);                                                                 \
        if (__r != 0) {                                                                   \
            fprintf(stderr, "%s:%d: %s: %s\n", __FILE__, __LINE__, #call,                 \
              wiredtiger_strerror(__r));                                                  \
            abort();                                                                      \
        }                                                                                 \
    } while (0)

static WT_CONNECTION *conn;
static int nthreads = 16, read_pct = 50, duration = 10, warmup = 2, nice_threads = 0;
static int idle_sessions = 0, use_ts = 0, value_len = 100;
static uint64_t nkeys = 100000;
static atomic_int phase; /* 0 warmup, 1 measure, 2 stop */
static atomic_uint_fast64_t commit_ts = 100, stable_ts = 0;

typedef struct {
    int id;
    pthread_t tid;
    uint64_t rnd;
    hist_t begin_r, begin_w, txn_r, txn_w;
    uint64_t reads, writes, rollbacks;
    uint64_t rb_read_ts, rb_conflict, rb_commit_ts, rb_commit;
} worker_t;

static uint64_t
rnd_next(uint64_t *s)
{
    uint64_t x = *s;

    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    return (*s = x);
}

static void
lower_priority(void)
{
#ifdef __linux__
    if (setpriority(PRIO_PROCESS, (id_t)syscall(SYS_gettid), 19) != 0)
        perror("setpriority");
#elif defined(__APPLE__)
    if (setpriority(PRIO_DARWIN_THREAD, 0, PRIO_DARWIN_BG) != 0)
        perror("setpriority");
#endif
}

static void *
worker(void *arg)
{
    WT_CURSOR *c;
    WT_SESSION *s;
    worker_t *w;
    uint64_t key, t0, t1, t2, ts;
    char *value;
    bool write;
    int p, ret;

    w = arg;
    if (w->id < nice_threads)
        lower_priority();
    CHECK(conn->open_session(conn, NULL, NULL, &s));
    CHECK(s->open_cursor(s, "table:bench", NULL, NULL, &c));
    value = malloc((size_t)value_len + 1);
    memset(value, 'a' + w->id % 26, (size_t)value_len);
    value[value_len] = '\0';

    while ((p = atomic_load_explicit(&phase, memory_order_relaxed)) != 2) {
        key = rnd_next(&w->rnd) % nkeys + 1;
        write = (int)(rnd_next(&w->rnd) % 100) >= read_pct;

        t0 = now_ns();
        CHECK(s->begin_transaction(s, NULL));
        t1 = now_ns();
        if (!write && use_ts && (ts = atomic_load(&stable_ts)) != 0 &&
          (ret = s->timestamp_transaction_uint(s, WT_TS_TXN_TYPE_READ, ts)) != 0) {
            if (ret != EINVAL)
                CHECK(ret);
            CHECK(s->rollback_transaction(s, NULL));
            if (p == 1) {
                w->rollbacks++;
                w->rb_read_ts++;
            }
            continue;
        }
        c->set_key(c, key);
        if (write) {
            c->set_value(c, value);
            ret = c->update(c);
        } else {
            ret = c->search(c);
            if (ret == WT_NOTFOUND)
                ret = 0;
        }
        if (ret == WT_ROLLBACK || ret == WT_PREPARE_CONFLICT) {
            CHECK(s->rollback_transaction(s, NULL));
            if (p == 1) {
                w->rollbacks++;
                w->rb_conflict++;
            }
            continue;
        }
        CHECK(ret);
        if (write && use_ts) {
            ts = atomic_fetch_add(&commit_ts, 1);
            ret = s->timestamp_transaction_uint(s, WT_TS_TXN_TYPE_COMMIT, ts);
            if (ret == EINVAL) {
                CHECK(s->rollback_transaction(s, NULL));
                if (p == 1) {
                    w->rollbacks++;
                    w->rb_commit_ts++;
                }
                continue;
            }
            CHECK(ret);
        }
        CHECK(c->reset(c));
        /*
         * With timestamps, the stable timestamp can pass a commit timestamp set by a thread that
         * was descheduled before its commit; the commit then fails and the transaction is rolled
         * back.
         */
        if ((ret = s->commit_transaction(s, NULL)) != 0) {
            if (!use_ts || ret != EINVAL)
                CHECK(ret);
            if (p == 1) {
                w->rollbacks++;
                w->rb_commit++;
            }
            continue;
        }
        t2 = now_ns();

        if (p != 1)
            continue;
        if (write) {
            hadd(&w->begin_w, t1 - t0);
            hadd(&w->txn_w, t2 - t0);
            w->writes++;
        } else {
            hadd(&w->begin_r, t1 - t0);
            hadd(&w->txn_r, t2 - t0);
            w->reads++;
        }
    }
    CHECK(s->close(s, NULL));
    free(value);
    return (NULL);
}

/*
 * Every millisecond, move the stable timestamp to TS_LAG behind all_durable and the oldest
 * timestamp to TS_LAG behind stable, never backwards. Commits with a timestamp at or below stable
 * and reads with a timestamp below oldest are rolled back and counted.
 */
#define TS_LAG 10000
static void *
ts_thread(void *arg)
{
    WT_SESSION *s;
    uint64_t durable, last;
    char buf[64], cfg[128];

    (void)arg;
    last = 1;
    CHECK(conn->open_session(conn, NULL, NULL, &s));
    while (atomic_load(&phase) != 2) {
        if (conn->query_timestamp(conn, buf, "get=all_durable") == 0) {
            durable = strtoull(buf, NULL, 16);
            if (durable > last + TS_LAG) {
                last = durable - TS_LAG;
                snprintf(cfg, sizeof(cfg), "oldest_timestamp=%" PRIx64 ",stable_timestamp=%" PRIx64,
                  last > TS_LAG ? last - TS_LAG : 1, last);
                CHECK(conn->set_timestamp(conn, cfg));
                atomic_store(&stable_ts, last);
            }
        }
        usleep(1000);
    }
    CHECK(s->close(s, NULL));
    return (NULL);
}

static const char *stat_names[] = {
  "transaction: transaction begins",
  "transaction: transaction walk of concurrent sessions",
  "transaction: sessions scanned in each walk of concurrent sessions",
  "transaction: transaction ID allocations retried after a snapshot stopped waiting",
  "lock: txn global read lock acquisitions",
  "lock: txn global write lock acquisitions",
  "lock: txn global lock application thread time waiting (usecs)",
  "lock: txn global lock internal thread time waiting (usecs)",
  "connection: pthread mutex shared lock read-lock calls",
  "connection: pthread mutex condition wait calls",
};
#define NSTATS (sizeof(stat_names) / sizeof(stat_names[0]))

static void
read_stats(WT_SESSION *s, int64_t *out)
{
    WT_CURSOR *c;
    const char *desc, *pval;
    int64_t v;
    size_t i;
    int ret;

    for (i = 0; i < NSTATS; i++)
        out[i] = -1;
    CHECK(s->open_cursor(s, "statistics:", NULL, NULL, &c));
    while ((ret = c->next(c)) == 0) {
        CHECK(c->get_value(c, &desc, &pval, &v));
        for (i = 0; i < NSTATS; i++)
            if (strcmp(desc, stat_names[i]) == 0)
                out[i] = v;
    }
    if (ret != WT_NOTFOUND)
        CHECK(ret);
    CHECK(c->close(c));
}

static void
print_hist(const char *name, const hist_t *h)
{
    printf(" %s_n=%" PRIu64 " %s_p50=%" PRIu64 " %s_p99=%" PRIu64 " %s_p999=%" PRIu64
           " %s_p9999=%" PRIu64 " %s_max=%" PRIu64,
      name, h->n, name, hq(h, 0.50), name, hq(h, 0.99), name, hq(h, 0.999), name, hq(h, 0.9999),
      name, h->max);
}

int
main(int argc, char *argv[])
{
    WT_CURSOR *c;
    WT_SESSION *s, **idle;
    pthread_t tst;
    worker_t *w;
    hist_t br, bw, tr, tw;
    int64_t st0[NSTATS], st1[NSTATS];
    uint64_t k, reads, writes, rollbacks, rb_read_ts, rb_conflict, rb_commit_ts, rb_commit;
    uint64_t t_start, t_end;
    size_t i;
    char cfg[512], *value;
    const char *home;
    int ch, j;

    home = NULL;
    while ((ch = getopt(argc, argv, "d:h:i:k:n:r:t:TW:")) != -1)
        switch (ch) {
        case 'd':
            duration = atoi(optarg);
            break;
        case 'h':
            home = optarg;
            break;
        case 'i':
            idle_sessions = atoi(optarg);
            break;
        case 'k':
            nkeys = strtoull(optarg, NULL, 10);
            break;
        case 'n':
            nice_threads = atoi(optarg);
            break;
        case 'r':
            read_pct = atoi(optarg);
            break;
        case 't':
            nthreads = atoi(optarg);
            break;
        case 'T':
            use_ts = 1;
            break;
        case 'W':
            warmup = atoi(optarg);
            break;
        default:
            fprintf(stderr,
              "usage: snapbench -h home [-t threads] [-r read%%] [-d secs] [-W warmup] [-k keys] "
              "[-n low-priority threads] [-i idle sessions] [-T]\n");
            return (1);
        }
    if (home == NULL) {
        fprintf(stderr, "snapbench: -h is required\n");
        return (1);
    }

    snprintf(cfg, sizeof(cfg),
      "create,cache_size=1GB,session_max=%d,statistics=(fast),"
      "eviction=(threads_min=4,threads_max=4)",
      nthreads + idle_sessions + 64);
    CHECK(wiredtiger_open(home, NULL, cfg, &conn));
    CHECK(conn->open_session(conn, NULL, NULL, &s));
    CHECK(s->create(s, "table:bench", "key_format=Q,value_format=S"));
    CHECK(s->open_cursor(s, "table:bench", NULL, "bulk", &c));
    value = malloc((size_t)value_len + 1);
    memset(value, 'x', (size_t)value_len);
    value[value_len] = '\0';
    for (k = 1; k <= nkeys; k++) {
        c->set_key(c, k);
        c->set_value(c, value);
        CHECK(c->insert(c));
    }
    CHECK(c->close(c));
    free(value);
    CHECK(s->checkpoint(s, NULL));

    idle = calloc((size_t)idle_sessions + 1, sizeof(*idle));
    for (j = 0; j < idle_sessions; j++)
        CHECK(conn->open_session(conn, NULL, NULL, &idle[j]));

    w = calloc((size_t)nthreads, sizeof(*w));
    atomic_store(&phase, 0);
    if (use_ts) {
        CHECK(conn->set_timestamp(conn, "oldest_timestamp=1,stable_timestamp=1"));
        CHECK(pthread_create(&tst, NULL, ts_thread, NULL) == 0 ? 0 : EINVAL);
    }
    for (j = 0; j < nthreads; j++) {
        w[j].id = j;
        w[j].rnd = 0x9E3779B97F4A7C15ULL * (uint64_t)(j + 1);
        CHECK(pthread_create(&w[j].tid, NULL, worker, &w[j]) == 0 ? 0 : EINVAL);
    }
    sleep((unsigned)warmup);
    read_stats(s, st0);
    t_start = now_ns();
    atomic_store(&phase, 1);
    sleep((unsigned)duration);
    atomic_store(&phase, 2);
    t_end = now_ns();
    read_stats(s, st1);
    for (j = 0; j < nthreads; j++)
        pthread_join(w[j].tid, NULL);
    if (use_ts)
        pthread_join(tst, NULL);

    memset(&br, 0, sizeof(br));
    memset(&bw, 0, sizeof(bw));
    memset(&tr, 0, sizeof(tr));
    memset(&tw, 0, sizeof(tw));
    reads = writes = rollbacks = rb_read_ts = rb_conflict = rb_commit_ts = rb_commit = 0;
    for (j = 0; j < nthreads; j++) {
        hmerge(&br, &w[j].begin_r);
        hmerge(&bw, &w[j].begin_w);
        hmerge(&tr, &w[j].txn_r);
        hmerge(&tw, &w[j].txn_w);
        reads += w[j].reads;
        writes += w[j].writes;
        rollbacks += w[j].rollbacks;
        rb_read_ts += w[j].rb_read_ts;
        rb_conflict += w[j].rb_conflict;
        rb_commit_ts += w[j].rb_commit_ts;
        rb_commit += w[j].rb_commit;
    }

    printf("threads=%d read_pct=%d nice=%d idle=%d ts=%d secs=%.3f reads=%" PRIu64
           " writes=%" PRIu64 " rollbacks=%" PRIu64 " rb_read_ts=%" PRIu64 " rb_conflict=%" PRIu64
           " rb_commit_ts=%" PRIu64 " rb_commit=%" PRIu64 " ops_per_sec=%.0f",
      nthreads, read_pct, nice_threads, idle_sessions, use_ts, (double)(t_end - t_start) / 1e9,
      reads, writes, rollbacks, rb_read_ts, rb_conflict, rb_commit_ts, rb_commit,
      (double)(reads + writes) * 1e9 / (double)(t_end - t_start));
    print_hist("begin_r", &br);
    print_hist("begin_w", &bw);
    print_hist("txn_r", &tr);
    print_hist("txn_w", &tw);
    for (i = 0; i < NSTATS; i++)
        printf(" stat%zu=%" PRId64, i, st0[i] < 0 || st1[i] < 0 ? -1 : st1[i] - st0[i]);
    printf("\n");
    fflush(stdout);

    /* Skip the final checkpoint so runs don't load the disk for the next one. */
    _exit(0);
}
