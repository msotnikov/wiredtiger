/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * Checkpoint pending stacks.
 *
 * A checkpoint gather that walks every open handle costs time proportional to the number of open
 * trees, while the work is proportional to the trees modified since the last checkpoint. The
 * pending stacks hold the trees that may need work from the next checkpoint, so the gather can
 * visit only those:
 * - a tree is pushed when it goes from clean to modified and when it is opened;
 * - the gather takes every stack, visits the taken trees and pushes back the trees that may need a
 *   later checkpoint: modified, in exclusive or special use, or with a clean checkpoint timer other
 *   than forever.
 *
 * A handle is the node of its stack and is in at most one stack, which its pending flag tracks.
 * While pending, the handle holds a reference, as a cached cursor does: sweep can close it but
 * does not free it. A handle is never removed from the middle of a stack, a stack is only taken
 * whole.
 *
 * The stacks are complete once a gather that walks every handle and fills the stacks has finished;
 * until then, and after a checkpoint that does not fill them, the gather walks every handle.
 */

/*
 * __checkpoint_pending_ignores --
 *     Return true for trees never pushed: the metadata, the history store, and in-memory, read-only
 *     and awaiting publication trees.
 */
static WT_INLINE bool
__checkpoint_pending_ignores(WT_BTREE *btree)
{
    return (WT_IS_ANY_METADATA(btree->dhandle) || __wti_checkpoint_tree_not_gathered(btree) ||
      F_ISSET_ATOMIC_32(btree, WT_BTREE_AWAITS_PUBLISH));
}

/*
 * __checkpoint_pending_push --
 *     Push a chain of handles, linked from first to last, on a pending stack.
 */
static void
__checkpoint_pending_push(
  WTI_CKPT_PENDING *pending, uint32_t slot, WT_DATA_HANDLE *first, WT_DATA_HANDLE *last)
{
    WTI_CKPT_PENDING_STACK *stack;
    WT_DATA_HANDLE *head;

    stack = &pending->stack[slot & (WTI_CKPT_PENDING_STACKS - 1)];
    do {
        head = __wt_atomic_load_ptr_relaxed(&stack->head);
        __wt_atomic_store_ptr_relaxed(&last->ckpt_pending_next, head);
    } while (!__wt_atomic_cas_ptr(&stack->head, head, first));
}

/*
 * __checkpoint_pending_add --
 *     Push a handle unless it is pending already. Return true if it was pushed.
 */
static bool
__checkpoint_pending_add(WTI_CKPT_PENDING *pending, uint32_t slot, WT_DATA_HANDLE *dhandle)
{
    if (!__wt_atomic_load_bool_acquire(&pending->enabled))
        return (false);

    /* Test before the compare and swap, a handle stays pending until a gather takes it. */
    if (__wt_atomic_load_uint8_relaxed(&dhandle->ckpt_pending) != 0 ||
      !__wt_atomic_cas_uint8(&dhandle->ckpt_pending, 0, 1))
        return (false);

    WT_DHANDLE_ACQUIRE(dhandle);
    __checkpoint_pending_push(pending, slot, dhandle, dhandle);
    return (true);
}

/*
 * __checkpoint_pending_take --
 *     Take every pending stack. The caller owns the returned chain, linked through
 *     ckpt_pending_next. Each handle in it is still pending and holds its reference.
 */
static WT_DATA_HANDLE *
__checkpoint_pending_take(WTI_CKPT_PENDING *pending, uint64_t *countp)
{
    WTI_CKPT_PENDING_STACK *stack;
    WT_DATA_HANDLE *chain, *head, *last, *next;
    uint64_t count;
    u_int i;

    chain = NULL;
    count = 0;
    for (i = 0; i < WTI_CKPT_PENDING_STACKS; ++i) {
        stack = &pending->stack[i];
        do {
            head = __wt_atomic_load_ptr_relaxed(&stack->head);
        } while (head != NULL && !__wt_atomic_cas_ptr(&stack->head, head, NULL));
        if (head == NULL)
            continue;

        for (last = head;; last = next) {
            ++count;
            if ((next = __wt_atomic_load_ptr_relaxed(&last->ckpt_pending_next)) == NULL)
                break;
        }
        __wt_atomic_store_ptr_relaxed(&last->ckpt_pending_next, chain);
        chain = head;
    }
    *countp = count;
    return (chain);
}

/*
 * __checkpoint_pending_return --
 *     Push back a chain taken from the pending stacks whose handles are still pending.
 */
static void
__checkpoint_pending_return(WTI_CKPT_PENDING *pending, uint32_t slot, WT_DATA_HANDLE *chain)
{
    WT_DATA_HANDLE *last, *next;

    if (chain == NULL)
        return;

    for (last = chain; (next = __wt_atomic_load_ptr_relaxed(&last->ckpt_pending_next)) != NULL;
      last = next)
        ;
    __checkpoint_pending_push(pending, slot, chain, last);
}

/*
 * __checkpoint_pending_unflag --
 *     Clear the pending flag of a taken handle. A writer that modifies the tree after this point
 *     pushes the handle again. The caller reads the tree's modified flag after the full barrier,
 *     and a writer reads the pending flag after setting the modified flag and a full barrier, so
 *     either the caller sees the tree modified or the writer pushes it. An opener orders the open
 *     flag the same way.
 */
static void
__checkpoint_pending_unflag(WT_DATA_HANDLE *dhandle)
{
    __wt_atomic_store_uint8_relaxed(&dhandle->ckpt_pending, 0);
    WT_FULL_BARRIER();
}

/*
 * __checkpoint_pending_keep --
 *     Return true if an open tree the gather has visited may need work from a later checkpoint: the
 *     tree is modified, or in exclusive or special use, or has a clean checkpoint timer other than
 *     forever (zero after the tree went through the usual path, or a time to recheck its
 *     checkpoints). A clean tree with the timer set forever needs no visit until it is modified or
 *     reopened, and both push it.
 */
static bool
__checkpoint_pending_keep(WT_DATA_HANDLE *dhandle)
{
    WT_BTREE *btree;

    /*
     * The handle is not locked. The flags only change with the handle locked exclusively, and an
     * exclusive user reopens the tree, which pushes it, so a stale read only adds or skips a visit
     * of a clean tree.
     */
    if (FLD_ISSET(__wt_tsan_suppress_load_uint16(&dhandle->flags), WT_DHANDLE_EXCLUSIVE))
        return (true);

    btree = dhandle->handle;
    if (FLD_ISSET(__wt_tsan_suppress_load_uint32(&btree->flags), WT_BTREE_SPECIAL_FLAGS))
        return (true);
    if (__checkpoint_pending_ignores(btree))
        return (false);
    if (__wt_atomic_load_bool_acquire(&btree->modified))
        return (true);
    return (
      __wt_atomic_load_uint64_relaxed(&btree->clean_ckpt_timer) != WT_BTREE_CLEAN_CKPT_FOREVER);
}

/*
 * __checkpoint_pending_release --
 *     Release a handle taken from the pending stacks, pushing it back first if a later checkpoint
 *     may need it. The caller has cleared the handle's pending flag.
 */
static void
__checkpoint_pending_release(WT_SESSION_IMPL *session, WT_DATA_HANDLE *dhandle)
{
    WTI_CKPT_PENDING *pending;

    pending = &S2C(session)->ckpt.pending;
    if (!__wt_conn_btree_apply_skip(dhandle) && __checkpoint_pending_keep(dhandle) &&
      __checkpoint_pending_add(pending, session->id, dhandle))
        ++pending->kept;
    WT_DHANDLE_RELEASE(dhandle);
}

/*
 * __checkpoint_pending_release_all --
 *     Release every handle of a chain taken from the pending stacks.
 */
static void
__checkpoint_pending_release_all(WT_SESSION_IMPL *session, WT_DATA_HANDLE *chain)
{
    WT_DATA_HANDLE *dhandle, *next;

    for (dhandle = chain; dhandle != NULL; dhandle = next) {
        next = __wt_atomic_load_ptr_relaxed(&dhandle->ckpt_pending_next);
        __checkpoint_pending_unflag(dhandle);
        __checkpoint_pending_release(session, dhandle);
    }
}

/*
 * __checkpoint_pending_take_all --
 *     Take every pending stack and count the taken handles.
 */
static WT_DATA_HANDLE *
__checkpoint_pending_take_all(WT_SESSION_IMPL *session)
{
    WTI_CKPT_PENDING *pending;
    WT_DATA_HANDLE *chain;
#ifdef HAVE_DIAGNOSTIC
    WT_DATA_HANDLE *dhandle;
#endif

    pending = &S2C(session)->ckpt.pending;
    chain = __checkpoint_pending_take(pending, &pending->taken);

#ifdef HAVE_DIAGNOSTIC
    /*
     * A pending handle holds a reference, so it stays in the connection's handle list and the chain
     * has no more handles than the list. A longer chain has a cycle.
     */
    WT_ASSERT(
      session, pending->taken <= __wt_atomic_load_uint64_relaxed(&S2C(session)->dhandle_count));
    for (dhandle = chain; dhandle != NULL;
      dhandle = __wt_atomic_load_ptr_relaxed(&dhandle->ckpt_pending_next))
        WT_ASSERT(session, __wt_atomic_load_uint8_relaxed(&dhandle->ckpt_pending) == 1);
#endif
    return (chain);
}

/*
 * __checkpoint_pending_disable --
 *     Stop pushing handles and release the pending handles. A gather that walks and fills the
 *     stacks enables them again.
 */
static void
__checkpoint_pending_disable(WT_SESSION_IMPL *session)
{
    WTI_CKPT_PENDING *pending;
    WT_DATA_HANDLE *dhandle, *next;

    pending = &S2C(session)->ckpt.pending;
    __wt_atomic_store_bool_release(&pending->enabled, false);
    WT_FULL_BARRIER();
    pending->complete = false;

    for (dhandle = __checkpoint_pending_take_all(session); dhandle != NULL; dhandle = next) {
        next = __wt_atomic_load_ptr_relaxed(&dhandle->ckpt_pending_next);
        __wt_atomic_store_uint8_relaxed(&dhandle->ckpt_pending, 0);
        WT_DHANDLE_RELEASE(dhandle);
    }
}

/*
 * __checkpoint_pending_gen_raise --
 *     Raise a tree's checkpoint generation to the given generation, never lowering it: a checkpoint
 *     may have set a later generation since the caller read the generation.
 */
static void
__checkpoint_pending_gen_raise(WT_SESSION_IMPL *session, WT_BTREE *btree, uint64_t gen)
{
    uint64_t old;

    WT_ASSERT(session, S2BT(session) == btree);

    for (old = __wt_atomic_load_uint64_acquire(&btree->checkpoint_gen); old < gen;
      old = __wt_atomic_load_uint64_acquire(&btree->checkpoint_gen))
        if (__wt_atomic_cas_uint64(&btree->checkpoint_gen, old, gen)) {
            WT_STAT_DSRC_SET(session, btree_checkpoint_generation, gen);
            break;
        }
}

/*
 * __checkpoint_pending_publish --
 *     Publish that the running checkpoint's gather from the pending stacks completed, then raise
 *     the checkpoint generation of the trees pushed since the gather took the stacks. Those trees
 *     are not part of this checkpoint, as the clean trees the full walk skips, so eviction in them
 *     need not be bound by the checkpoint snapshot. A writer reads the published generation after
 *     pushing its tree, so either the writer raises the tree's generation or this walk finds the
 *     tree.
 */
static void
__checkpoint_pending_publish(WT_SESSION_IMPL *session, uint64_t gen)
{
    WTI_CKPT_PENDING *pending;
    WT_BTREE *btree;
    WT_DATA_HANDLE *dhandle;
    u_int i;

    pending = &S2C(session)->ckpt.pending;
    __wt_atomic_store_uint64_release(&pending->gather_gen, gen);
    WT_FULL_BARRIER();

    /*
     * Only the checkpoint takes from the stacks, so the handles below each head stay linked while
     * this walk reads them, and each holds a reference.
     */
    for (i = 0; i < WTI_CKPT_PENDING_STACKS; ++i)
        for (dhandle = __wt_atomic_load_ptr_acquire(&pending->stack[i].head); dhandle != NULL;
          dhandle = __wt_atomic_load_ptr_relaxed(&dhandle->ckpt_pending_next)) {
            if (__wt_conn_btree_apply_skip(dhandle))
                continue;
            btree = dhandle->handle;
            if (__checkpoint_pending_ignores(btree) ||
              __wt_atomic_load_uint64_relaxed(&btree->ckpt_gather_gen) == gen)
                continue;
            WT_WITH_DHANDLE(session, dhandle, __checkpoint_pending_gen_raise(session, btree, gen));
        }
}

#ifdef HAVE_DIAGNOSTIC
/*
 * __checkpoint_pending_verify --
 *     Check that the gather from the pending stacks missed no modified tree: every open modified
 *     tree was visited by the gather or is pending. A writer sets the modified flag before it
 *     pushes the tree, so wait out a writer between the two steps.
 */
static void
__checkpoint_pending_verify(WT_SESSION_IMPL *session, uint64_t gen)
{
    WT_BTREE *btree;
    WT_CONNECTION_IMPL *conn;
    WT_DATA_HANDLE *dhandle;
    u_int i;

    conn = S2C(session);
    WT_WITH_HANDLE_LIST_READ_LOCK(session, {
        TAILQ_FOREACH (dhandle, &conn->dhqh, q) {
            if (__wt_conn_btree_apply_skip(dhandle))
                continue;
            btree = dhandle->handle;
            if (__checkpoint_pending_ignores(btree))
                continue;
            for (i = 0; i < 10 * WT_THOUSAND; ++i) {
        if (!__wt_atomic_load_bool_acquire(&btree->modified) ||
          __wt_atomic_load_uint8_acquire(&dhandle->ckpt_pending) != 0 ||
          __wt_atomic_load_uint64_relaxed(&btree->ckpt_gather_gen) == gen)
            break;
        __wt_sleep(0, WT_THOUSAND);
            }
            WT_ASSERT_ALWAYS(session, i < 10 * WT_THOUSAND,
              "modified tree %s is neither pending nor visited by the checkpoint gather",
              dhandle->name);
}
});
}
#endif

/*
 * __checkpoint_gather_pending --
 *     Checkpoint handle gather that visits only the handles taken from the pending stacks. The
 *     checkpoint transaction's snapshot is taken before the gather, and a tree is pushed before any
 *     of its updates commit, so every update the checkpoint must include is in a taken tree.
 */
static int
__checkpoint_gather_pending(WT_SESSION_IMPL *session,
  int (*file_func)(WT_SESSION_IMPL *, const char *[]),
  int (*skip_func)(WT_SESSION_IMPL *, WT_DATA_HANDLE *, bool *), const char *cfg[],
  uint64_t *walkedp)
{
    WTI_CKPT_PENDING *pending;
    WT_DATA_HANDLE *dhandle, *next;
    WT_DECL_RET;
    uint64_t gen;

    pending = &S2C(session)->ckpt.pending;
    gen = __wt_gen(session, WT_GEN_CHECKPOINT);
    *walkedp = 0;

    /*
     * The handles later in the chain are still pending, so writers don't push them again; each one
     * holds a reference, so it is not freed while the chain is walked.
     */
    for (dhandle = __checkpoint_pending_take_all(session); dhandle != NULL; dhandle = next) {
        next = __wt_atomic_load_ptr_relaxed(&dhandle->ckpt_pending_next);
        __checkpoint_pending_unflag(dhandle);

        if (!__wt_conn_btree_apply_skip(dhandle)) {
            ++*walkedp;
            if ((ret = __wt_conn_btree_apply_one(session, dhandle, file_func, skip_func, cfg)) !=
              0) {
                (void)__checkpoint_pending_add(pending, session->id, dhandle);
                WT_DHANDLE_RELEASE(dhandle);
                __checkpoint_pending_return(pending, session->id, next);
                return (ret);
            }
        }
        __checkpoint_pending_release(session, dhandle);
    }

    __checkpoint_pending_publish(session, gen);
#ifdef HAVE_DIAGNOSTIC
    __checkpoint_pending_verify(session, gen);
#endif
    return (0);
}

/*
 * __wti_checkpoint_pending_mode_set --
 *     Choose how the next checkpoint handle gather finds the trees to visit. The stacks are not
 *     used on a disaggregated connection, in recovery or by the checkpoint at close, and the
 *     metadata walk of a named or drop checkpoint does not fill them: those disable the stacks, and
 *     the next gather walks every handle and fills them. A forced checkpoint walks every handle and
 *     fills the stacks.
 */
void
__wti_checkpoint_pending_mode_set(WT_SESSION_IMPL *session, bool force, bool meta_walk)
{
    WT_CONNECTION_IMPL *conn;
    WTI_CKPT_PENDING *pending;

    conn = S2C(session);
    pending = &conn->ckpt.pending;

    WT_ASSERT(session, WT_SESSION_IS_CHECKPOINT(session));
    WT_ASSERT(session, !pending->complete || __wt_atomic_load_bool_relaxed(&pending->enabled));

    if (__wt_conn_is_disagg(session) || F_ISSET(conn, WT_CONN_RECOVERING) ||
      F_ISSET_ATOMIC_32(conn, WT_CONN_CLOSING_CHECKPOINT) || meta_walk) {
        pending->mode = WTI_CKPT_GATHER_WALK;
        __checkpoint_pending_disable(session);
    } else if (pending->complete && !force &&
      !FLD_ISSET(conn->debug.flags, WT_CONN_DEBUG_CKPT_GATHER_WALK))
        pending->mode = WTI_CKPT_GATHER_PENDING;
    else
        pending->mode = WTI_CKPT_GATHER_WALK_FILL;
}

/*
 * __wti_checkpoint_pending_prune --
 *     Release the pending handles of trees a later checkpoint no longer needs to visit. Called when
 *     a checkpoint is skipped, so idle handles sweep has closed do not stay referenced until the
 *     next checkpoint that gathers handles.
 */
void
__wti_checkpoint_pending_prune(WT_SESSION_IMPL *session)
{
    WT_ASSERT_SPINLOCK_OWNED(session, &S2C(session)->checkpoint_lock);

    if (!__wt_atomic_load_bool_relaxed(&S2C(session)->ckpt.pending.enabled))
        return;
    __checkpoint_pending_release_all(session, __checkpoint_pending_take_all(session));
}

/*
 * __wt_checkpoint_gather_start --
 *     Start a checkpoint handle gather. Gather from the pending stacks and set done, or prepare a
 *     walk of every handle. A walk that fills the stacks enables them and takes the pending
 *     handles, so their references are not held through checkpoints that do not use the stacks. The
 *     walk reads each tree after enabling the stacks and a full barrier, and a writer reads the
 *     enabled flag after setting the modified flag and a full barrier, so either the walk sees the
 *     tree modified or the writer pushes it.
 */
int
__wt_checkpoint_gather_start(WT_SESSION_IMPL *session,
  int (*file_func)(WT_SESSION_IMPL *, const char *[]),
  int (*skip_func)(WT_SESSION_IMPL *, WT_DATA_HANDLE *, bool *), const char *cfg[],
  uint64_t *walkedp, bool *donep)
{
    WTI_CKPT_PENDING *pending;

    pending = &S2C(session)->ckpt.pending;
    *donep = false;

    WT_ASSERT_SPINLOCK_OWNED(session, &S2C(session)->schema_lock);
    WT_ASSERT(session, pending->chain == NULL);

    pending->taken = pending->kept = 0;
    switch (pending->mode) {
    case WTI_CKPT_GATHER_PENDING:
        WT_STAT_CONN_INCR(session, checkpoint_handle_gather_pending);
        *donep = true;
        return (__checkpoint_gather_pending(session, file_func, skip_func, cfg, walkedp));
    case WTI_CKPT_GATHER_WALK_FILL:
        __wt_atomic_store_bool_release(&pending->enabled, true);
        WT_FULL_BARRIER();
        pending->chain = __checkpoint_pending_take_all(session);
        break;
    case WTI_CKPT_GATHER_WALK:
        break;
    }
    WT_STAT_CONN_INCR(session, checkpoint_handle_gather_walk);
    return (0);
}

/*
 * __wt_checkpoint_gather_visited --
 *     A walk of every handle visited a handle: push it if a later checkpoint may need it.
 */
void
__wt_checkpoint_gather_visited(WT_SESSION_IMPL *session, WT_DATA_HANDLE *dhandle)
{
    WTI_CKPT_PENDING *pending;

    pending = &S2C(session)->ckpt.pending;
    if (pending->mode == WTI_CKPT_GATHER_WALK_FILL && __checkpoint_pending_keep(dhandle) &&
      __checkpoint_pending_add(pending, session->id, dhandle))
        ++pending->kept;
}

/*
 * __wt_checkpoint_gather_end --
 *     End a checkpoint handle gather. A successful walk that filled the stacks makes them complete;
 *     a failed one did not visit every handle, so the next gather walks again.
 */
void
__wt_checkpoint_gather_end(WT_SESSION_IMPL *session, bool success)
{
    WTI_CKPT_PENDING *pending;

    pending = &S2C(session)->ckpt.pending;
    if (pending->mode == WTI_CKPT_GATHER_WALK_FILL) {
        __checkpoint_pending_release_all(session, pending->chain);
        pending->chain = NULL;
        pending->complete = success;
    }
    WT_STAT_CONN_SET(session, checkpoint_handle_pending_taken, pending->taken);
    WT_STAT_CONN_SET(session, checkpoint_handle_pending_kept, pending->kept);
}

/*
 * __wt_checkpoint_pending_clear --
 *     Stop pushing handles and release the pending handles before the connection discards its data
 *     handles.
 */
void
__wt_checkpoint_pending_clear(WT_SESSION_IMPL *session)
{
    WT_ASSERT(session, F_ISSET_ATOMIC_32(S2C(session), WT_CONN_CLOSING_NO_MORE_OPENS));

    __checkpoint_pending_disable(session);
}

/*
 * __wt_checkpoint_tree_opened --
 *     A tree was opened: push it. An opened tree has a zero clean checkpoint timer, so the next
 *     checkpoint must visit it. The full barrier orders the handle's open flag before the pending
 *     flag is read, see __checkpoint_pending_unflag.
 */
void
__wt_checkpoint_tree_opened(WT_SESSION_IMPL *session, WT_BTREE *btree)
{
    WT_FULL_BARRIER();

    if (!__checkpoint_pending_ignores(btree) &&
      __checkpoint_pending_add(&S2C(session)->ckpt.pending, session->id, btree->dhandle))
        WT_STAT_CONN_INCR(session, checkpoint_handle_pending_added);
}

/*
 * __wt_checkpoint_tree_modified --
 *     A tree went from clean to modified: push it. The caller has set the tree's modified flag and
 *     issued a full barrier, see __checkpoint_pending_unflag.
 *
 * If the running checkpoint finished a gather from the pending stacks that did not visit the tree,
 *     the tree is not part of that checkpoint: raise its checkpoint generation, as the full walk
 *     does for a clean tree it skips, so eviction in the tree is not bound by the checkpoint
 *     snapshot. Raise it to the generation checked here: a checkpoint that started since then has
 *     not visited the tree.
 */
void
__wt_checkpoint_tree_modified(WT_SESSION_IMPL *session, WT_BTREE *btree)
{
    WTI_CKPT_PENDING *pending;
    uint64_t gen;

    if (__checkpoint_pending_ignores(btree))
        return;

    pending = &S2C(session)->ckpt.pending;
    if (__checkpoint_pending_add(pending, session->id, btree->dhandle))
        WT_STAT_CONN_INCR(session, checkpoint_handle_pending_added);

    gen = __wt_gen(session, WT_GEN_CHECKPOINT);
    if (__wt_atomic_load_uint64_acquire(&pending->gather_gen) != gen ||
      __wt_atomic_load_uint64_relaxed(&btree->ckpt_gather_gen) == gen)
        return;
    __checkpoint_pending_gen_raise(session, btree, gen);
}

#ifdef HAVE_UNITTEST
bool
__ut_checkpoint_pending_add(WTI_CKPT_PENDING *pending, uint32_t slot, WT_DATA_HANDLE *dhandle)
{
    return (__checkpoint_pending_add(pending, slot, dhandle));
}

WT_DATA_HANDLE *
__ut_checkpoint_pending_take(WTI_CKPT_PENDING *pending, uint64_t *countp)
{
    return (__checkpoint_pending_take(pending, countp));
}

void
__ut_checkpoint_pending_return(WTI_CKPT_PENDING *pending, uint32_t slot, WT_DATA_HANDLE *chain)
{
    __checkpoint_pending_return(pending, slot, chain);
}

void
__ut_checkpoint_pending_unflag(WT_DATA_HANDLE *dhandle)
{
    __checkpoint_pending_unflag(dhandle);
}
#endif
