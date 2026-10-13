/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

#define WTI_CHECKPOINT_SESSION_FLAGS (WT_SESSION_CAN_WAIT | WT_SESSION_IGNORE_CACHE_SIZE)
#define WTI_CKPT_FOREACH_NAME_OR_ORDER(ckptbase, ckpt) \
    for ((ckpt) = (ckptbase); (ckpt)->name != NULL || (ckpt)->order != 0; ++(ckpt))

/*
 * Inactive should always be 0. Other states are roughly ordered by appearance in the checkpoint
 * life cycle.
 */
typedef enum {
    WTI_CHECKPOINT_STATE_INACTIVE,
    WTI_CHECKPOINT_STATE_APPLY_META,
    WTI_CHECKPOINT_STATE_APPLY_BTREE,
    WTI_CHECKPOINT_STATE_UPDATE_OLDEST,
    WTI_CHECKPOINT_STATE_SYNC_FILE,
    WTI_CHECKPOINT_STATE_EVICT_FILE,
    WTI_CHECKPOINT_STATE_BM_SYNC,
    WTI_CHECKPOINT_STATE_RESOLVE,
    WTI_CHECKPOINT_STATE_POSTPROCESS,
    WTI_CHECKPOINT_STATE_HS,
    WTI_CHECKPOINT_STATE_HS_SYNC,
    WTI_CHECKPOINT_STATE_COMMIT,
    WTI_CHECKPOINT_STATE_META_CKPT,
    WTI_CHECKPOINT_STATE_META_SYNC,
    WTI_CHECKPOINT_STATE_ROLLBACK,
    WTI_CHECKPOINT_STATE_LOG,
    WTI_CHECKPOINT_STATE_CKPT_TREE,
    WTI_CHECKPOINT_STATE_ACTIVE,
    WTI_CHECKPOINT_STATE_ESTABLISH,
    WTI_CHECKPOINT_STATE_START_TXN
} WTI_CHECKPOINT_STATE;

/*
 * WTI_CKPT_HANDLE_STATS --
 *     Statistics related to handles.
 */
struct __wti_ckpt_handle_stats {
    uint64_t apply;           /* handles applied */
    uint64_t apply_time;      /* applied handles gather time */
    uint64_t drop;            /* handle checkpoints dropped */
    uint64_t drop_time;       /* handle checkpoints dropped time */
    uint64_t lock;            /* handles locked */
    uint64_t lock_time;       /* handles locked time */
    uint64_t meta_check;      /* handles metadata check */
    uint64_t meta_check_time; /* handles metadata check time */
    uint64_t skip;            /* handles skipped */
    uint64_t skip_time;       /* skipped handles gather time */
};

/*
 * WTI_CKPT_GATHER_MODE --
 *     How the checkpoint handle gather finds the trees to visit.
 */
typedef enum {
    /* Walk every open handle, the pending stacks are not used. */
    WTI_CKPT_GATHER_WALK,
    /* Walk every open handle and fill the pending stacks for the next checkpoint. */
    WTI_CKPT_GATHER_WALK_FILL,
    /* Visit only the handles taken from the pending stacks. */
    WTI_CKPT_GATHER_PENDING
} WTI_CKPT_GATHER_MODE;

/*
 * WTI_CKPT_PENDING_STACK --
 *     A lock-free stack of data handles, linked through the handles. Handles are pushed one at a
 *     time and the checkpoint takes the whole stack at once, so the stack has no ABA problem. Each
 *     stack is on its own cache line.
 */
struct __wti_ckpt_pending_stack {
    WT_CACHE_LINE_PAD_BEGIN
    /* Last handle pushed, or NULL */
    wt_shared WT_DATA_HANDLE *head;
    WT_CACHE_LINE_PAD_END
};

/* Number of pending stacks, a power of two. A session pushes on stack (id & (count - 1)). */
#define WTI_CKPT_PENDING_STACKS 16

/*
 * WTI_CKPT_PENDING --
 *     Trees that may need work from the next checkpoint: trees modified or opened since the last
 *     checkpoint, and trees the last checkpoint left work for. Fields other than the stacks, the
 *     enabled flag and the gather generation belong to the checkpoint, which changes them under
 *     the checkpoint lock.
 */
struct __wti_ckpt_pending {
    WTI_CKPT_PENDING_STACK stack[WTI_CKPT_PENDING_STACKS];
    /* Handles are pushed, set by a gather that walks and fills the stacks */
    wt_shared bool enabled;
    /* The stacks hold every tree the next gather must visit */
    bool complete;
    /* Gather mode of the running checkpoint */
    WTI_CKPT_GATHER_MODE mode;
    /* Handles a walking gather took from the stacks */
    WT_DATA_HANDLE *chain;
    /* Handles the running gather took from the stacks */
    uint64_t taken;
    /* Handles the running gather pushed back */
    uint64_t kept;
    /* Checkpoint generation whose gather visited only the pending handles and completed */
    wt_shared uint64_t gather_gen;
};

/*
 * WTI_CKPT_PROGRESS --
 *     Checkpoint progress.
 */
struct __wti_ckpt_progress {
    uint64_t files_checkpointed; /* files successfully checkpointed */
    uint64_t msg_count;          /* number of progress messages */
    uint64_t pages_visited;      /* pages visited during checkpoint */
    uint64_t write_bytes;        /* bytes written during checkpoint */
    uint64_t write_pages;        /* pages written during checkpoint */
};

/*
 * WTI_CKPT_THREAD --
 *     Checkpoint server information.
 */
struct __wti_ckpt_thread {
    WT_CONDVAR *cond;         /* wait mutex */
    WT_SESSION_IMPL *session; /* session associated with thread */
    wt_thread_t tid;          /* thread id */
    bool tid_set;             /* thread set */
#define WT_CKPT_LOGSIZE(conn) (__wt_atomic_load_int64_relaxed(&(conn)->ckpt.server.logsize) != 0)
    wt_shared wt_off_t logsize; /* thread log size period */
    bool signalled;             /* thread signalled */
    uint64_t usecs;             /* thread timer */
};

/*
 * WTI_CKPT_TIMER --
 *     Time-related statistics.
 */
struct __wti_ckpt_timer {
    struct timespec timer_end;
    struct timespec timer_start;
    uint64_t max;
    uint64_t min;
    uint64_t recent;
    uint64_t total;
};

/* DO NOT EDIT: automatically built by prototypes.py: BEGIN */

extern bool __wti_checkpoint_tree_not_gathered(WT_BTREE *btree)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_checkpoint_parallel_commit(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_checkpoint_parallel_release_snapshot(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_checkpoint_parallel_rollback(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern void __wti_checkpoint_pending_mode_set(WT_SESSION_IMPL *session, bool force, bool meta_walk);
extern void __wti_checkpoint_pending_prune(WT_SESSION_IMPL *session);

#ifdef HAVE_UNITTEST

#endif

/* DO NOT EDIT: automatically built by prototypes.py: END */
