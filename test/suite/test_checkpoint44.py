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

import time, wttest
from wiredtiger import stat

# Test the checkpoint pending stacks with clean trees that still need checkpoint work and with
# operations that take a handle exclusively.
@wttest.skip_for_hook("disagg", "the checkpoint pending stacks are not used with disaggregated storage")
class test_checkpoint44(wttest.WiredTigerTestCase):
    ntables = 10
    uri = 'table:checkpoint44.'
    conn_config = 'statistics=(all)'

    def get_stat(self, s):
        c = self.session.open_cursor('statistics:', None, None)
        val = c[s][2]
        c.close()
        return val

    def write(self, i, value):
        c = self.session.open_cursor(self.uri + str(i))
        c[1] = value
        c.close()

    def check(self, expected):
        for i, value in expected.items():
            c = self.session.open_cursor(self.uri + str(i), None, 'checkpoint=WiredTigerCheckpoint')
            self.assertEqual(c[1], value, 'table {}'.format(i))
            c.close()

    def create_tables(self):
        expected = {}
        for i in range(self.ntables):
            self.session.create(self.uri + str(i), 'key_format=i,value_format=i')
            self.write(i, 0)
            expected[i] = 0
        self.session.checkpoint()
        return expected

    def checkpoint_count(self, i):
        c = self.session.open_cursor('metadata:', None, None)
        config = c['file:checkpoint44.' + str(i) + '.wt']
        c.close()
        return config.count('WiredTigerCheckpoint.')

    # A checkpoint cursor keeps a checkpoint of table 0 in use, so later checkpoints cannot delete
    # it and write table 0 again although it is clean: it stays pending. Once the cursor is closed,
    # a checkpoint deletes the old checkpoints of table 0 without table 0 being modified; later
    # checkpoints stop writing table 0, and it leaves the stacks.
    def test_checkpoint_in_use(self):
        expected = self.create_tables()
        held = self.session.open_cursor(self.uri + '0', None, 'checkpoint=WiredTigerCheckpoint')
        for value in range(1, 4):
            self.write(0, value)
            self.session.checkpoint()
        expected[0] = 3
        self.assertGreater(self.checkpoint_count(0), 1)

        for value in range(1, 4):
            self.write(1, value)
            self.session.checkpoint()
            self.assertEqual(self.get_stat(stat.conn.checkpoint_handle_walked), 2)
            self.assertEqual(self.get_stat(stat.conn.checkpoint_handle_applied), 2)
        expected[1] = 3
        held.close()

        self.write(1, 4)
        self.session.checkpoint()
        self.assertEqual(self.checkpoint_count(0), 1)
        applied = []
        for value in range(5, 10):
            self.write(1, value)
            self.session.checkpoint()
            applied.append(self.get_stat(stat.conn.checkpoint_handle_applied))
        self.assertEqual(applied[-2:], [1, 1])
        self.assertEqual(self.get_stat(stat.conn.checkpoint_handle_walked), 1)
        expected[1] = 9
        self.check(expected)

    # Verify, alter and a bulk load take a handle exclusively, closing and opening the tree. The
    # tree must still be visited when it is next modified.
    def test_exclusive(self):
        expected = self.create_tables()
        self.write(0, 1)
        self.write(1, 1)
        self.session.checkpoint()
        expected[0] = expected[1] = 1

        self.session.verify(self.uri + '0')
        self.write(0, 2)
        self.session.checkpoint()
        expected[0] = 2
        self.check(expected)

        self.session.alter(self.uri + '1', 'app_metadata="checkpoint44"')
        self.write(1, 2)
        self.session.checkpoint()
        expected[1] = 2
        self.check(expected)

        bulk = self.ntables
        self.session.create(self.uri + str(bulk), 'key_format=i,value_format=i')
        c = self.session.open_cursor(self.uri + str(bulk), None, 'bulk')
        c[1] = 1
        c.close()
        self.session.checkpoint()
        expected[bulk] = 1
        self.check(expected)

        self.write(bulk, 2)
        self.session.checkpoint()
        expected[bulk] = 2
        self.check(expected)
        self.assertGreater(self.get_stat(stat.conn.checkpoint_handle_gather_pending), 0)

# A tree opened again after sweep closed it is pushed, so the next checkpoint visits it although it
# is not modified: it deletes the old checkpoints a checkpoint cursor kept in use.
@wttest.skip_for_hook("disagg", "the checkpoint pending stacks are not used with disaggregated storage")
class test_checkpoint44_reopen(wttest.WiredTigerTestCase):
    uri = 'table:checkpoint44_reopen.'
    conn_config = 'statistics=(all),' + \
        'file_manager=(close_idle_time=1,close_scan_interval=1,close_handle_minimum=0)'

    def get_stat(self, s):
        c = self.session.open_cursor('statistics:', None, None)
        val = c[s][2]
        c.close()
        return val

    def write(self, i, value):
        c = self.session.open_cursor(self.uri + str(i))
        c[1] = value
        c.close()

    def checkpoint_count(self):
        c = self.session.open_cursor('metadata:', None, None)
        config = c['file:checkpoint44_reopen.0.wt']
        c.close()
        return config.count('WiredTigerCheckpoint.')

    def test_reopen(self):
        for i in range(3):
            self.session.create(self.uri + str(i), 'key_format=i,value_format=i')
            self.write(i, 0)
        self.session.checkpoint()
        held = self.session.open_cursor(self.uri + '0', None, 'checkpoint=WiredTigerCheckpoint')
        for value in range(1, 4):
            self.write(0, value)
            self.session.checkpoint()
        held.close()

        # Wait for sweep to close the idle tables, then let checkpoints drop table 0 from the
        # stacks while it is closed.
        start = self.get_stat(stat.conn.dh_sweep_expired_close)
        for _ in range(100):
            if self.get_stat(stat.conn.dh_sweep_expired_close) >= start + 3:
                break
            time.sleep(0.1)
        for value in range(1, 3):
            self.write(1, value)
            self.session.checkpoint()
        self.assertGreater(self.checkpoint_count(), 1)

        # Read table 0, which opens it, and checkpoint without modifying it.
        c = self.session.open_cursor(self.uri + '0')
        self.assertEqual(c[1], 3)
        c.close()
        for value in range(3, 5):
            self.write(1, value)
            self.session.checkpoint()
        self.assertEqual(self.checkpoint_count(), 1)
