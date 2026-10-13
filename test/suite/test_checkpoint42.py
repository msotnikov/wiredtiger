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

# Test the checkpoint lists: once a full handle walk has made them complete, a checkpoint visits
# only the trees modified or opened since the last checkpoint, plus the trees the last checkpoint
# left work for. Each round modifies a few random tables and checks every table through a
# checkpoint cursor, so a tree the gather missed shows up as a stale value. Sweep closes idle
# handles every second, tables are dropped and created again between checkpoints, and forced and
# named checkpoints walk every handle in between.
class test_checkpoint42(wttest.WiredTigerTestCase):
    ntables = 100
    nkeys = 10
    rounds = 8
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

    def write(self, i, round):
        c = self.session.open_cursor(self.uri + str(i))
        for k in range(self.nkeys):
            c[k] = round * 1000 + k
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

        modified = []
        for round in range(1, self.rounds + 1):
            # Drop a table the last checkpoint wrote, which is still in the lists, and create it
            # again.
            if round % 3 == 0:
                i = modified[0]
                self.session.drop(self.uri + str(i))
                self.session.create(self.uri + str(i), 'key_format=i,value_format=i')
                self.write(i, round)
                expected[i] = round

            modified = rnd.sample(range(self.ntables), 5)
            for i in modified:
                self.write(i, round)
                expected[i] = round

            # Let sweep close the clean handles, including handles the lists hold.
            if round % 2 == 0:
                time.sleep(2)

            # Open every table, then take the other checkpoint, which visits every tree.
            if self.other_ckpt is not None and round == 5:
                for i in range(self.ntables):
                    c = self.session.open_cursor(self.uri + str(i))
                    self.assertEqual(c[0], expected[i] * 1000)
                    c.close()
                self.session.checkpoint(self.other_ckpt)
            self.session.checkpoint()
            self.check(expected, 'checkpoint=WiredTigerCheckpoint')

            # The checkpoint visits the modified trees and the trees the last checkpoint wrote,
            # not every open handle. The first checkpoints after a forced or named one visit
            # every tree.
            walked = self.get_stat(stat.conn.checkpoint_handle_walked)
            if round in (2, 3, self.rounds):
                self.assertLess(walked, self.ntables // 2)

        self.reopen_conn()
        self.check(expected, None)
