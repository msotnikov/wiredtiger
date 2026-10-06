#!/usr/bin/env python
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
#
# Test the configuration that enables/disables dirty table flushing.
#

import wiredtiger, wttest
from wiredtiger import stat

class test_config09(wttest.WiredTigerTestCase):
    ntables = 50
    nentries = 5
    uri = 'table:config09.'
    conn_config = 'hash=(buckets=256,dhandle_buckets=1024),statistics=(fast)'

    # Create, populate and checkpoint the initial tables.
    def create_tables(self):
        for i in range(self.ntables):
            uri = self.uri + str(i)
            self.session.create(uri, 'key_format=i,value_format=i')
            c = self.session.open_cursor(uri)
            for j in range(self.nentries):
                c[j] = j
            c.close()
        self.session.checkpoint()

    # Update half the tables.
    def update_tables(self):
        for i in range(self.ntables//2):
            uri = self.uri + str(i)
            c = self.session.open_cursor(uri)
            for j in range(self.nentries):
                c[j] = j + 100
            c.close()
        self.session.checkpoint()

    # Verify statistics.
    def test_config09_invalid(self):
        self.conn.close()

        # Verify the message when using non-power-of-two values.
        msg = '/power of 2/'
        config = 'hash=(buckets=255,dhandle_buckets=1024)'
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.wiredtiger_open('.', config), msg)
        config = 'hash=(buckets=256,dhandle_buckets=1000)'
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.wiredtiger_open('.', config), msg)
        config = 'hash=(dhandle_buckets=1000)'
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.wiredtiger_open('.', config), msg)
        config = 'hash=(session_dhandle_buckets=1000)'
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.wiredtiger_open('.', config), msg)

        # A non-zero session size must be at least 64.
        config = 'hash=(session_dhandle_buckets=32)'
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.wiredtiger_open('.', config), msg)

    # Session handle arrays smaller and larger than the connection's.
    def test_config09_session_buckets(self):
        self.conn.close()

        for session_buckets in (128, 4096):
            config = 'create,hash=(dhandle_buckets=1024,session_dhandle_buckets={})'.format(
                session_buckets)
            conn = self.wiredtiger_open('.', config)
            session = conn.open_session()
            for i in range(self.ntables):
                uri = self.uri + str(i)
                session.create(uri, 'key_format=i,value_format=i')
                c = session.open_cursor(uri)
                c[i] = session_buckets
                c.close()

            # Open every cursor again: the handles come from the session's handle cache.
            for i in range(self.ntables):
                c = session.open_cursor(self.uri + str(i))
                self.assertEqual(c[i], session_buckets)
                c.close()
            session.checkpoint()
            conn.close()

    def test_config09(self):
        self.create_tables()

        val = self.get_stat(stat.conn.buckets)
        self.assertEqual(val, 256)
        val = self.get_stat(stat.conn.buckets_dh)
        self.assertEqual(val, 1024)

        self.update_tables()
        applied = self.get_stat(stat.conn.checkpoint_handle_applied)
        # We cannot assert it is equal to half because there could be other
        # internal tables in the count. Assert it is less than 75% and at least
        # half.
        self.assertGreaterEqual(applied, self.ntables // 2)
        self.assertLess(applied, self.ntables // 4 * 3)
        skipped = self.get_stat(stat.conn.checkpoint_handle_skipped)
        self.assertNotEqual(skipped, 0)

        # The handle stats are reset at the start of each gather and then set
        # to the per-checkpoint value. The gather walks only the hash buckets
        # flagged since the previous checkpoint, so a checkpoint with no
        # changes visits no more handles than the one before it; without the
        # reset the value would grow.
        locked_1 = self.get_stat(stat.conn.checkpoint_handle_locked)
        meta_checked_1 = self.get_stat(stat.conn.checkpoint_handle_meta_checked)
        self.session.checkpoint()
        locked_2 = self.get_stat(stat.conn.checkpoint_handle_locked)
        meta_checked_2 = self.get_stat(stat.conn.checkpoint_handle_meta_checked)
        self.assertLessEqual(locked_2, locked_1)
        self.assertLessEqual(meta_checked_2, meta_checked_1)

        # Trees checkpointed by the update keep their buckets flagged for one
        # more checkpoint, which sets their clean checkpoint timer. After that
        # a checkpoint with no changes visits no handle.
        self.session.checkpoint()
        self.assertEqual(self.get_stat(stat.conn.checkpoint_handle_walked), 0)
        self.assertEqual(self.get_stat(stat.conn.checkpoint_handle_locked), 0)
        self.assertEqual(self.get_stat(stat.conn.checkpoint_handle_meta_checked), 0)

        self.conn.close()
