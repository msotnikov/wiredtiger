/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * The checkpoint pending stacks: handles are pushed one at a time, each at most once while pending,
 * and the checkpoint takes every stack at once. These tests drive the stacks on handles that are
 * not linked into a connection, single-threaded and with pushers racing a taker.
 */

#include <catch2/catch.hpp>

#include <atomic>
#include <thread>
#include <vector>

#include "wt_internal.h"

namespace {

/* Take every stack and return the handles in the chain, clearing their pending flags. */
std::vector<WT_DATA_HANDLE *>
take_all(WTI_CKPT_PENDING *pending, uint64_t *countp)
{
    std::vector<WT_DATA_HANDLE *> taken;
    WT_DATA_HANDLE *dhandle, *next;

    for (dhandle = __ut_checkpoint_pending_take(pending, countp); dhandle != nullptr;
      dhandle = next) {
        next = dhandle->ckpt_pending_next;
        __ut_checkpoint_pending_unflag(dhandle);
        taken.push_back(dhandle);
    }
    return (taken);
}

} // namespace

TEST_CASE("Checkpoint pending: a handle is pushed once while pending", "[checkpoint_pending]")
{
    WTI_CKPT_PENDING pending{};
    WT_DATA_HANDLE dhandles[3]{};
    uint64_t count;

    /* Nothing is pushed until the stacks are enabled. */
    REQUIRE_FALSE(__ut_checkpoint_pending_add(&pending, 0, &dhandles[0]));
    REQUIRE(dhandles[0].references == 0);

    pending.enabled = true;
    REQUIRE(__ut_checkpoint_pending_add(&pending, 0, &dhandles[0]));
    REQUIRE_FALSE(__ut_checkpoint_pending_add(&pending, 5, &dhandles[0]));
    REQUIRE(__ut_checkpoint_pending_add(&pending, 1, &dhandles[1]));
    REQUIRE(__ut_checkpoint_pending_add(&pending, 17, &dhandles[2]));
    REQUIRE(dhandles[0].references == 1);
    REQUIRE(dhandles[0].ckpt_pending == 1);

    /* Taking every stack returns each handle once, still pending. */
    WT_DATA_HANDLE *chain = __ut_checkpoint_pending_take(&pending, &count);
    REQUIRE(count == 3);
    int seen[3] = {0, 0, 0};
    for (WT_DATA_HANDLE *dhandle = chain; dhandle != nullptr;
      dhandle = dhandle->ckpt_pending_next) {
        REQUIRE(dhandle->ckpt_pending == 1);
        ++seen[dhandle - dhandles];
    }
    REQUIRE((seen[0] == 1 && seen[1] == 1 && seen[2] == 1));

    /* A returned chain is taken again, unchanged. */
    __ut_checkpoint_pending_return(&pending, 3, chain);
    std::vector<WT_DATA_HANDLE *> taken = take_all(&pending, &count);
    REQUIRE(count == 3);
    REQUIRE(taken.size() == 3);
    REQUIRE(__ut_checkpoint_pending_take(&pending, &count) == nullptr);
    REQUIRE(count == 0);

    /* Once its pending flag is cleared, a handle is pushed again. */
    REQUIRE(__ut_checkpoint_pending_add(&pending, 0, &dhandles[0]));
    REQUIRE(dhandles[0].references == 2);
    taken = take_all(&pending, &count);
    REQUIRE(taken.size() == 1);
    REQUIRE(taken[0] == &dhandles[0]);
}

TEST_CASE("Checkpoint pending: pushers race a taker", "[checkpoint_pending]")
{
    const int nhandles = 1000, nthreads = 8, nrounds = 200;
    WTI_CKPT_PENDING pending{};
    std::vector<WT_DATA_HANDLE> dhandles(nhandles);
    std::vector<std::atomic<uint64_t>> taken(nhandles);
    std::atomic<uint64_t> pushed{0};
    std::atomic<bool> done{false};

    pending.enabled = true;

    /* Every pusher pushes every handle in every round, so most pushes find the handle pending. */
    std::vector<std::thread> pushers;
    for (int t = 0; t < nthreads; ++t)
        pushers.emplace_back([&, t]() {
            for (int r = 0; r < nrounds; ++r)
                for (int i = 0; i < nhandles; ++i)
                    if (__ut_checkpoint_pending_add(
                          &pending, static_cast<uint32_t>(t), &dhandles[(i + t * 37) % nhandles]))
                        ++pushed;
        });

    /* The taker releases each taken handle's reference, as the checkpoint gather does. */
    auto take_once = [&]() {
        uint64_t count;
        WT_DATA_HANDLE *dhandle, *next;

        dhandle = __ut_checkpoint_pending_take(&pending, &count);
        REQUIRE(count <= static_cast<uint64_t>(nhandles));
        for (; dhandle != nullptr; dhandle = next) {
            next = dhandle->ckpt_pending_next;
            __ut_checkpoint_pending_unflag(dhandle);
            ++taken[dhandle - dhandles.data()];
            WT_DHANDLE_RELEASE(dhandle);
        }
    };
    std::thread taker([&]() {
        while (!done.load())
            take_once();
    });

    for (auto &t : pushers)
        t.join();
    done = true;
    taker.join();
    take_once();

    /* Every push was taken exactly once and every reference was released. */
    uint64_t total = 0;
    for (int i = 0; i < nhandles; ++i) {
        REQUIRE(dhandles[i].references == 0);
        REQUIRE(dhandles[i].ckpt_pending == 0);
        REQUIRE(taken[i] >= 1);
        total += taken[i];
    }
    REQUIRE(total == pushed);
}
