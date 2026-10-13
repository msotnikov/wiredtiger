#!/usr/bin/env python3
#
# Public Domain 2014-present MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#
# This is free and unencumbered software released into the public domain.
#
# Anyone is free to copy, modify, publish, use, compile, sell, or
# distribute this software, either in source code form or as a compiled
# binary, for any purpose, commercial or non-commercial, and by any
# means.
#
# In jurisdictions that recognize copyright laws, the author or authors
# of this software dedicate any and all copyright interest in the
# software to the public domain. We make this dedication for the benefit
# of the public at large and to the detriment of our heirs and
# successors. We intend this dedication to be an overt act of
# relinquishment in perpetuity of all present and future rights to this
# software under copyright law.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

import random, threading, time
import wttest
from wiredtiger import stat
from wtscenario import make_scenarios

# Test the checkpoint pending stacks while a writer thread modifies random tables during the
# checkpoints, so trees go from clean to modified while the gather takes and visits the stacks. The
# writer commits with increasing timestamps. Each checkpoint is taken at a stable timestamp, and
# every table is checked through a checkpoint cursor against the last value committed at or before
# it. A delay in checkpoint prepare, between the checkpoint snapshot and the handle gather, widens
# the window in which the writer modifies trees the gather then takes.
@wttest.skip_for_hook("disagg", "the checkpoint pending stacks are not used with disaggregated storage")
@wttest.skip_for_hook("rollback", "the writer thread does not retry rolled back operations")
class test_checkpoint43(wttest.WiredTigerTestCase):
    ntables = 200
    nkeys = 5
    nckpts = 10
    uri = 'table:checkpoint43.'

    scenarios = make_scenarios([
        ('fuzzy', dict(precise='false')),
        ('precise', dict(precise='true')),
    ])

    def conn_config(self):
        return 'statistics=(all),precise_checkpoint={},'.format(self.precise) + \
            'timing_stress_for_test=[prepare_checkpoint_delay]'

    def get_stat(self, s):
        c = self.session.open_cursor('statistics:', None, None)
        val = c[s][2]
        c.close()
        return val

    def writer(self, done, lock, state):
        try:
            session = self.conn.open_session()
            rnd = random.Random(43)
            while not done.is_set():
                i = rnd.randrange(self.ntables)
                with lock:
                    state['ts'] += 1
                    ts = state['ts']
                    session.begin_transaction()
                    c = session.open_cursor(self.uri + str(i))
                    for k in range(self.nkeys):
                        c[k] = ts
                    c.close()
                    session.commit_transaction('commit_timestamp=' + self.timestamp_str(ts))
                    state['history'][i].append(ts)
                time.sleep(0.001)
            session.close()
        except Exception as e:
            state['error'] = e

    def test_checkpoint43(self):
        lock = threading.Lock()
        state = {'ts': 1, 'history': [[1] for _ in range(self.ntables)], 'error': None}
        for i in range(self.ntables):
            self.session.create(self.uri + str(i), 'key_format=i,value_format=i')
            self.session.begin_transaction()
            c = self.session.open_cursor(self.uri + str(i))
            for k in range(self.nkeys):
                c[k] = 1
            c.close()
            self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(1))
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(1))
        self.session.checkpoint()

        done = threading.Event()
        thread = threading.Thread(target=self.writer, args=(done, lock, state))
        thread.start()
        gathers = self.get_stat(stat.conn.checkpoint_handle_gather_pending)
        try:
            for _ in range(self.nckpts):
                time.sleep(0.05)
                with lock:
                    stable = state['ts']
                    self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(stable))
                    expected = [h[-1] for h in state['history']]
                    state['history'] = [[ts] for ts in expected]
                self.session.checkpoint()
                for i in range(self.ntables):
                    c = self.session.open_cursor(
                        self.uri + str(i), None, 'checkpoint=WiredTigerCheckpoint')
                    for k in range(self.nkeys):
                        self.assertEqual(c[k], expected[i], 'table {} key {}'.format(i, k))
                    c.close()
                self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(stable))
        finally:
            done.set()
            thread.join()
        self.assertIsNone(state['error'])
        self.assertEqual(
            self.get_stat(stat.conn.checkpoint_handle_gather_pending) - gathers, self.nckpts)
