/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

/*
 * Spin locks:
 *
 * These used for cases where fast mutual exclusion is needed (where operations done while holding
 * the spin lock are expected to complete in a small number of instructions.
 */

/*
 * WT_SPIN_SESSION_ID_SAFE --
 *     Get the session ID. We need this because there are a few calls to lock and unlock where the
 * session parameter is actually NULL.
 */
#define WT_SPIN_SESSION_ID_SAFE(session) ((session) != NULL ? (session)->id : WT_SESSION_ID_NULL)

/*
 * __spin_init_internal --
 *     Initialize the WT portion of a spinlock.
 */
static WT_INLINE void
__spin_init_internal(WT_SPINLOCK *t, const char *name)
{
    t->name = name;
    __wt_atomic_store_uint32_relaxed(&t->session_id, WT_SESSION_ID_INVALID);
    t->stat_count_off = t->stat_app_usecs_off = t->stat_int_usecs_off = -1;
    t->stat_session_usecs_off = -1;
    t->initialized = 1;
}

#if SPINLOCK_TYPE == SPINLOCK_GCC

/* Default to spinning 1000 times before yielding. */
#ifndef WT_SPIN_COUNT
#define WT_SPIN_COUNT WT_THOUSAND
#endif

/*
 * __wt_spin_init --
 *     Initialize a spinlock.
 */
static WT_INLINE int
__wt_spin_init(WT_SESSION_IMPL *session, WT_SPINLOCK *t, const char *name)
{
    WT_UNUSED(session);

    t->lock = 0;
    __spin_init_internal(t, name);
    return (0);
}

/*
 * __wt_spin_destroy --
 *     Destroy a spinlock.
 */
static WT_INLINE void
__wt_spin_destroy(WT_SESSION_IMPL *session, WT_SPINLOCK *t)
{
    WT_UNUSED(session);

    t->lock = 0;
}

/*
 * __wt_spin_trylock --
 *     Try to lock a spinlock or fail immediately if it is busy.
 */
static WT_INLINE int
__wt_spin_trylock(WT_SESSION_IMPL *session, WT_SPINLOCK *t)
{
    WT_UNUSED(session);

    if (!__atomic_test_and_set(&t->lock, __ATOMIC_ACQUIRE)) {
        t->session_id = WT_SPIN_SESSION_ID_SAFE(session);
        return (0);
    } else
        return (EBUSY);
}

/*
 * __wt_spin_lock --
 *     Spin until the lock is acquired.
 */
static WT_INLINE void
__wt_spin_lock(WT_SESSION_IMPL *session, WT_SPINLOCK *t)
{
    int i;

    WT_UNUSED(session);

    while (__atomic_test_and_set(&t->lock, __ATOMIC_ACQUIRE)) {
        for (i = 0; t->lock && i < WT_SPIN_COUNT; i++)
            WT_PAUSE();
        if (t->lock)
            __wt_yield();
    }

    t->session_id = WT_SPIN_SESSION_ID_SAFE(session);
}

/*
 * __wt_spin_unlock --
 *     Release the spinlock.
 */
static WT_INLINE void
__wt_spin_unlock(WT_SESSION_IMPL *session, WT_SPINLOCK *t)
{
    WT_UNUSED(session);

    t->session_id = WT_SESSION_ID_INVALID;
    __atomic_clear(&t->lock, __ATOMIC_RELEASE);
}

#elif SPINLOCK_TYPE == SPINLOCK_PTHREAD_MUTEX || SPINLOCK_TYPE == SPINLOCK_PTHREAD_MUTEX_ADAPTIVE

/*
 * __wt_spin_init --
 *     Initialize a spinlock.
 */
static WT_INLINE int
__wt_spin_init(WT_SESSION_IMPL *session, WT_SPINLOCK *t, const char *name)
{
#if SPINLOCK_TYPE == SPINLOCK_PTHREAD_MUTEX_ADAPTIVE
    WT_DECL_RET;
    pthread_mutexattr_t attr;

    WT_RET(pthread_mutexattr_init(&attr));
    ret = pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_ADAPTIVE_NP);
    if (ret == 0)
        ret = pthread_mutex_init(&t->lock, &attr);
    WT_TRET(pthread_mutexattr_destroy(&attr));
    WT_RET(ret);
#else
    WT_RET(pthread_mutex_init(&t->lock, NULL));
#endif
    __spin_init_internal(t, name);

    WT_UNUSED(session);
    return (0);
}

/*
 * __wt_spin_destroy --
 *     Destroy a spinlock.
 */
static WT_INLINE void
__wt_spin_destroy(WT_SESSION_IMPL *session, WT_SPINLOCK *t)
{
    WT_UNUSED(session);

    if (t->initialized) {
        (void)pthread_mutex_destroy(&t->lock);
        t->initialized = 0;
    }
}

#if SPINLOCK_TYPE == SPINLOCK_PTHREAD_MUTEX || SPINLOCK_TYPE == SPINLOCK_PTHREAD_MUTEX_ADAPTIVE

/*
 * __wt_spin_trylock --
 *     Try to lock a spinlock or fail immediately if it is busy.
 */
static WT_INLINE int
__wt_spin_trylock(WT_SESSION_IMPL *session, WT_SPINLOCK *t)
{
    WT_DECL_RET;
    WT_UNUSED(session);

    ret = pthread_mutex_trylock(&t->lock);
    if (ret == 0)
        __wt_atomic_store_uint32_relaxed(&t->session_id, WT_SPIN_SESSION_ID_SAFE(session));
    return (ret);
}

/*
 * __wt_spin_lock --
 *     Spin until the lock is acquired.
 */
static WT_INLINE void
__wt_spin_lock(WT_SESSION_IMPL *session, WT_SPINLOCK *t)
{
    WT_DECL_RET;

    if ((ret = pthread_mutex_lock(&t->lock)) != 0)
        WT_IGNORE_RET(__wt_panic(session, ret, "pthread_mutex_lock: %s", t->name));
    __wt_atomic_store_uint32_relaxed(&t->session_id, WT_SPIN_SESSION_ID_SAFE(session));
}
#endif

/*
 * __wt_spin_unlock --
 *     Release the spinlock.
 */
static WT_INLINE void
__wt_spin_unlock(WT_SESSION_IMPL *session, WT_SPINLOCK *t)
{
    WT_DECL_RET;

    __wt_atomic_store_uint32_relaxed(&t->session_id, WT_SESSION_ID_INVALID);
    if ((ret = pthread_mutex_unlock(&t->lock)) != 0)
        WT_IGNORE_RET(__wt_panic(session, ret, "pthread_mutex_unlock: %s", t->name));
}

#elif SPINLOCK_TYPE == SPINLOCK_MSVC

/*
 * __wt_spin_init --
 *     Initialize a spinlock.
 */
static WT_INLINE int
__wt_spin_init(WT_SESSION_IMPL *session, WT_SPINLOCK *t, const char *name)
{
    DWORD windows_error;

    if (InitializeCriticalSectionAndSpinCount(&t->lock, 4 * WT_THOUSAND) == 0) {
        windows_error = __wt_getlasterror();
        __wt_errx(session, "%s: InitializeCriticalSectionAndSpinCount: %s", name,
          __wt_formatmessage(session, windows_error));
        return (__wt_map_windows_error(windows_error));
    }

    __spin_init_internal(t, name);
    return (0);
}

/*
 * __wt_spin_destroy --
 *     Destroy a spinlock.
 */
static WT_INLINE void
__wt_spin_destroy(WT_SESSION_IMPL *session, WT_SPINLOCK *t)
{
    WT_UNUSED(session);

    if (t->initialized) {
        DeleteCriticalSection(&t->lock);
        t->initialized = 0;
    }
}

/*
 * __wt_spin_trylock --
 *     Try to lock a spinlock or fail immediately if it is busy.
 */
static WT_INLINE int
__wt_spin_trylock(WT_SESSION_IMPL *session, WT_SPINLOCK *t)
{
    WT_UNUSED(session);

    BOOL b = TryEnterCriticalSection(&t->lock);
    if (b == 0)
        return (EBUSY);
    t->session_id = WT_SPIN_SESSION_ID_SAFE(session);
    return (0);
}

/*
 * __wt_spin_lock --
 *     Spin until the lock is acquired.
 */
static WT_INLINE void
__wt_spin_lock(WT_SESSION_IMPL *session, WT_SPINLOCK *t)
{
    WT_UNUSED(session);

    EnterCriticalSection(&t->lock);
    t->session_id = WT_SPIN_SESSION_ID_SAFE(session);
}

/*
 * __wt_spin_unlock --
 *     Release the spinlock.
 */
static WT_INLINE void
__wt_spin_unlock(WT_SESSION_IMPL *session, WT_SPINLOCK *t)
{
    WT_UNUSED(session);

    t->session_id = WT_SESSION_ID_INVALID;
    LeaveCriticalSection(&t->lock);
}

#else

#error Unknown spinlock type

#endif

/*
 * __wt_spin_locked --
 *     Check whether the spinlock is locked, irrespective of which session locked it.
 */
static WT_INLINE bool
__wt_spin_locked(WT_SESSION_IMPL *session, WT_SPINLOCK *t)
{
    WT_UNUSED(session);
    return (__wt_atomic_load_uint32_relaxed(&t->session_id) != WT_SESSION_ID_INVALID);
}

/*
 * __wt_spin_owned --
 *     Check whether the session owns the spinlock.
 */
static WT_INLINE bool
__wt_spin_owned(WT_SESSION_IMPL *session, WT_SPINLOCK *t)
{
    return (__wt_atomic_load_uint32_relaxed(&t->session_id) == WT_SPIN_SESSION_ID_SAFE(session));
}

/*
 * __wt_spin_unlock_if_owned --
 *     Unlock the spinlock only if it is acquired by the specified session.
 */
static WT_INLINE void
__wt_spin_unlock_if_owned(WT_SESSION_IMPL *session, WT_SPINLOCK *t)
{
    if (__wt_spin_owned(session, t))
        __wt_spin_unlock(session, t);
}

/*
 * WT_ASSERT_SPINLOCK_OWNED --
 *      Assert that the session owns the spinlock.
 */
#define WT_ASSERT_SPINLOCK_OWNED(session, t) WT_ASSERT((session), __wt_spin_owned((session), (t)));

/*
 * WT_SPIN_INIT_TRACKED --
 *	Spinlock initialization, with tracking.
 *
 * Implemented as a macro so we can pass in a statistics field and convert
 * it into a statistics structure array offset.
 */
#define WT_SPIN_INIT_TRACKED(session, t, name)                                                    \
    do {                                                                                          \
        WT_ERR(__wt_spin_init(session, t, #name));                                                \
        (t)->stat_count_off =                                                                     \
          (int16_t)WT_STATS_FIELD_TO_OFFSET(S2C(session)->stats, lock_##name##_count);            \
        (t)->stat_app_usecs_off =                                                                 \
          (int16_t)WT_STATS_FIELD_TO_OFFSET(S2C(session)->stats, lock_##name##_wait_application); \
        (t)->stat_int_usecs_off =                                                                 \
          (int16_t)WT_STATS_FIELD_TO_OFFSET(S2C(session)->stats, lock_##name##_wait_internal);    \
    } while (0)

#define WT_SPIN_INIT_SESSION_TRACKED(session, t, name)                                      \
    do {                                                                                    \
        WT_SPIN_INIT_TRACKED(session, t, name);                                             \
        (t)->stat_session_usecs_off =                                                       \
          (int16_t)WT_SESSION_STATS_FIELD_TO_OFFSET(&(session)->stats, lock_##name##_wait); \
    } while (0)

/*
 * __wt_spin_lock_track --
 *     Spinlock acquisition, with tracking.
 */
static WT_INLINE void
__wt_spin_lock_track(WT_SESSION_IMPL *session, WT_SPINLOCK *t)
{
    uint64_t time_diff, time_start, time_stop;
    int64_t *session_stats, **stats;

    if (t->stat_count_off != -1 && WT_STAT_ENABLED(session)) {
        time_start = __wt_clock(session);
        __wt_spin_lock(session, t);
        time_stop = __wt_clock(session);
        time_diff = WT_CLOCKDIFF_US(time_stop, time_start);
        stats = (int64_t **)S2C(session)->stats;
        session_stats = (int64_t *)&(session->stats);
        stats[session->stat_conn_bucket][t->stat_count_off]++;
        if (F_ISSET(session, WT_SESSION_INTERNAL))
            stats[session->stat_conn_bucket][t->stat_int_usecs_off] += (int64_t)time_diff;
        else {
            stats[session->stat_conn_bucket][t->stat_app_usecs_off] += (int64_t)time_diff;
        }

        /*
         * Not all spin locks increment session statistics. Check whether the offset is initialized
         * to determine whether they are enabled.
         */
        if (t->stat_session_usecs_off != -1)
            session_stats[t->stat_session_usecs_off] += (int64_t)time_diff;
    } else
        __wt_spin_lock(session, t);
}

/*
 * __wt_spin_trylock_track --
 *     Try to lock a spinlock or fail immediately if it is busy. Track if successful.
 */
static WT_INLINE int
__wt_spin_trylock_track(WT_SESSION_IMPL *session, WT_SPINLOCK *t)
{
    int64_t **stats;

    if (t->stat_count_off != -1 && WT_STAT_ENABLED(session)) {
        WT_RET(__wt_spin_trylock(session, t));
        stats = (int64_t **)S2C(session)->stats;
        stats[session->stat_conn_bucket][t->stat_count_off]++;
        return (0);
    }
    return (__wt_spin_trylock(session, t));
}

/*
 * __wt_schema_write_wait_stat --
 *     Add the time a writer waited in the schema lock queue to the schema lock wait statistics.
 */
static WT_INLINE void
__wt_schema_write_wait_stat(WT_SESSION_IMPL *session, uint64_t time_start)
{
    WT_RWLOCK *l;
    int64_t *session_stats, **stats, time_diff;

    l = &S2C(session)->schema_lock;
    time_diff = (int64_t)WT_CLOCKDIFF_US(__wt_clock(session), time_start);
    stats = (int64_t **)S2C(session)->stats;
    if (F_ISSET(session, WT_SESSION_INTERNAL))
        __wt_atomic_add_int64_relaxed(
          &stats[session->stat_conn_bucket][l->stat_int_usecs_off], time_diff);
    else
        __wt_atomic_add_int64_relaxed(
          &stats[session->stat_conn_bucket][l->stat_app_usecs_off], time_diff);
    session_stats = (int64_t *)&(session->stats);
    if (l->stat_session_usecs_off != -1)
        __wt_atomic_add_int64_relaxed(&session_stats[l->stat_session_usecs_off], time_diff);
}

/*
 * __wt_schema_writelock --
 *     Acquire the schema lock for write.
 */
static WT_INLINE void
__wt_schema_writelock(WT_SESSION_IMPL *session, WT_SCHEMA_WAITER *waiter)
{
    WT_CONNECTION_IMPL *conn;
    WT_FUTEX_WORD wake_val;
    WT_SCHEMA_WAITER *prev;
    uint64_t time_start;
    int pause_cnt;

    conn = S2C(session);
    waiter->next = NULL;
    waiter->ready = 0;

    /*
     * Queue writers in arrival order before the rwlock. A rwlock unlock broadcasts to every thread
     * waiting on its condition variable; with the queue, at most one writer waits on the rwlock and
     * each handoff wakes only the next writer.
     */
    do
        prev = __atomic_load_n(&conn->schema_write_tail, __ATOMIC_ACQUIRE);
    while (!__atomic_compare_exchange_n(
      &conn->schema_write_tail, &prev, waiter, false, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE));

    if (prev != NULL) {
        time_start = WT_STAT_ENABLED(session) ? __wt_clock(session) : 0;
        __atomic_store_n(&prev->next, waiter, __ATOMIC_RELEASE);
        for (pause_cnt = 0; __atomic_load_n(&waiter->ready, __ATOMIC_ACQUIRE) == 0; ++pause_cnt)
            if (pause_cnt < 100)
                WT_PAUSE();
            else
                WT_IGNORE_RET(__wt_futex_wait(&waiter->ready, 0, WT_MILLION, &wake_val));
        if (time_start != 0)
            __wt_schema_write_wait_stat(session, time_start);
    }
    __wt_writelock(session, &conn->schema_lock);
}

/*
 * __wt_schema_queue_leave --
 *     Remove a writer from the head of the schema lock queue and wake the next writer.
 */
static WT_INLINE void
__wt_schema_queue_leave(WT_SESSION_IMPL *session, WT_SCHEMA_WAITER *waiter)
{
    WT_SCHEMA_WAITER *expected, *next;

    if ((next = __atomic_load_n(&waiter->next, __ATOMIC_ACQUIRE)) == NULL) {
        expected = waiter;
        if (__atomic_compare_exchange_n(&S2C(session)->schema_write_tail, &expected, NULL, false,
              __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE))
            return;
        /* A writer swapped itself in as the tail and has not linked itself to this waiter yet. */
        while ((next = __atomic_load_n(&waiter->next, __ATOMIC_ACQUIRE)) == NULL)
            WT_PAUSE();
    }
    WT_IGNORE_RET(__wt_futex_wake(&next->ready, WT_FUTEX_WAKE_ONE, 1));
}

/*
 * __wt_schema_try_writelock --
 *     Try to acquire the schema lock for write, fail immediately if it is busy.
 */
static WT_INLINE int
__wt_schema_try_writelock(WT_SESSION_IMPL *session, WT_SCHEMA_WAITER *waiter)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    WT_SCHEMA_WAITER *expected;

    conn = S2C(session);
    waiter->next = NULL;
    waiter->ready = 0;
    expected = NULL;
    if (!__atomic_compare_exchange_n(
          &conn->schema_write_tail, &expected, waiter, false, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE))
        return (EBUSY);
    if ((ret = __wt_try_writelock(session, &conn->schema_lock)) != 0)
        __wt_schema_queue_leave(session, waiter);
    return (ret);
}

/*
 * __wt_schema_writeunlock --
 *     Release the schema lock held for write.
 */
static WT_INLINE void
__wt_schema_writeunlock(WT_SESSION_IMPL *session, WT_SCHEMA_WAITER *waiter)
{
    __wt_writeunlock(session, &S2C(session)->schema_lock);
    __wt_schema_queue_leave(session, waiter);
}

/*
 * __wt_schema_readlock --
 *     Acquire the schema lock for read.
 */
static WT_INLINE void
__wt_schema_readlock(WT_SESSION_IMPL *session, WT_SCHEMA_WAITER *waiter)
{
    WT_UNUSED(waiter);
    __wt_readlock(session, &S2C(session)->schema_lock);
}

/*
 * __wt_schema_readunlock --
 *     Release the schema lock held for read.
 */
static WT_INLINE void
__wt_schema_readunlock(WT_SESSION_IMPL *session, WT_SCHEMA_WAITER *waiter)
{
    WT_UNUSED(waiter);
    __wt_readunlock(session, &S2C(session)->schema_lock);
}
