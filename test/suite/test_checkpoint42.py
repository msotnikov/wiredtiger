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

import random, time
import wttest
from wiredtiger import stat
from wtscenario import make_scenarios

# Test the checkpoint pending stacks: once a walk of every handle has filled them, a checkpoint
# visits only the trees modified or opened since the last checkpoint, plus the trees the last
# checkpoint left work for. Each step modifies a few random tables and checks every table through
# a checkpoint cursor, so a tree the gather missed shows up as a stale value. Sweep closes idle
# handles, a table the last checkpoint wrote is dropped and created again, and a forced or named
# checkpoint walks every handle in between. At the end every table is dropped, and the next
# checkpoint must release every dropped tree.
@wttest.skip_for_hook("disagg", "the checkpoint pending stacks are not used with disaggregated storage")
class test_checkpoint42(wttest.WiredTigerTestCase):
    ntables = 100
    nkeys = 10
    nmodify = 5
    nsteps = 8
    uri = 'table:checkpoint42.'
    conn_config = 'statistics=(all),' + \
        'file_manager=(close_idle_time=1,close_scan_interval=1,close_handle_minimum=0)'

    scenarios = make_scenarios([
        ('plain', dict(other_ckpt=None)),
        ('forced', dict(other_ckpt='force=true')),
        ('named', dict(other_ckpt='name=checkpoint42')),
    ])

    def get_stat(self, s):
        c = self.session.open_cursor('statistics:', None, None)
        val = c[s][2]
        c.close()
        return val

    # Wait until sweep has closed more idle handles.
    def wait_for_sweep_close(self):
        start = self.get_stat(stat.conn.dh_sweep_expired_close)
        for _ in range(300):
            time.sleep(0.1)
            if self.get_stat(stat.conn.dh_sweep_expired_close) > start:
                return
        self.fail('sweep closed no idle handle')

    def write(self, i, step):
        c = self.session.open_cursor(self.uri + str(i))
        for k in range(self.nkeys):
            c[k] = step * 1000 + k
        c.close()

    def check(self, expected, cfg):
        for i in range(self.ntables):
            c = self.session.open_cursor(self.uri + str(i), None, cfg)
            for k in range(self.nkeys):
                self.assertEqual(c[k], expected[i] * 1000 + k, 'table {} key {}'.format(i, k))
            c.close()

    def test_checkpoint42(self):
        rnd = random.Random(42)
        expected = []
        for i in range(self.ntables):
            self.session.create(self.uri + str(i), 'key_format=i,value_format=i')
            self.write(i, 0)
            expected.append(0)
        self.session.checkpoint()
        self.check(expected, 'checkpoint=WiredTigerCheckpoint')

        last_modified = []
        for step in range(1, self.nsteps + 1):
            # Drop a table the last checkpoint wrote, which is still pending, and create it again.
            if step % 3 == 0:
                i = last_modified[0]
                self.session.drop(self.uri + str(i))
                self.session.create(self.uri + str(i), 'key_format=i,value_format=i')
                self.write(i, step)
                expected[i] = step

            modified = set(rnd.sample(range(self.ntables), self.nmodify))
            for i in modified:
                self.write(i, step)
                expected[i] = step

            # Let sweep close idle handles, including pending ones.
            if step % 2 == 0:
                self.wait_for_sweep_close()

            # Open every table, then take the other checkpoint, which visits every tree.
            if self.other_ckpt is not None and step == 5:
                for i in range(self.ntables):
                    c = self.session.open_cursor(self.uri + str(i))
                    self.assertEqual(c[0], expected[i] * 1000)
                    c.close()
                self.session.checkpoint(self.other_ckpt)

            gathers = self.get_stat(stat.conn.checkpoint_handle_gather_pending)
            self.session.checkpoint()
            self.check(expected, 'checkpoint=WiredTigerCheckpoint')

            # A named checkpoint disables the stacks, so the checkpoint after it walks every handle.
            from_pending = self.get_stat(stat.conn.checkpoint_handle_gather_pending) > gathers
            self.assertEqual(from_pending, not (self.other_ckpt == 'name=checkpoint42' and step == 5))

            # The gather visits every modified tree. Besides those it visits the trees the last
            # checkpoint wrote and the table created again, until a forced or named checkpoint
            # leaves every tree for the next checkpoints to visit.
            if step in (2, 3, 4, self.nsteps):
                walked = self.get_stat(stat.conn.checkpoint_handle_walked)
                self.assertGreaterEqual(walked, len(modified))
                self.assertLessEqual(walked, 3 * self.nmodify)
            last_modified = list(modified)

        self.reopen_conn()
        self.check(expected, None)

        # Write every table, then drop them. The checkpoint after the drop takes the dropped trees
        # from the stacks and keeps none of them, so it releases their references.
        self.session.checkpoint()
        for i in range(self.ntables):
            self.write(i, self.nsteps + 1)
        self.session.checkpoint()
        for i in range(self.ntables):
            self.session.drop(self.uri + str(i))
        self.session.checkpoint()
        self.assertGreaterEqual(self.get_stat(stat.conn.checkpoint_handle_pending_taken), self.ntables)
        self.assertEqual(self.get_stat(stat.conn.checkpoint_handle_pending_kept), 0)
