"""
Copyright 2014 Basho Technologies, Inc.

This file is provided to you under the Apache License,
Version 2.0 (the "License"); you may not use this file
except in compliance with the License.  You may obtain
a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""

import platform
if platform.python_version() < '2.7':
    unittest = __import__('unittest2')
else:
    import unittest
from riak.tests import RUN_CONSISTENCY


class StrongConsistencyTests(object):
    @unittest.skipUnless(RUN_CONSISTENCY, 'RUN_CONSISTENCY is undefined')
    def test_consistency(self):
        """
        Test the case where strong consistency has been enabled.
        """
        btype = self.client.bucket_type(self.consistent_bucket)
        bucket = btype.bucket(self.consistent_bucket)
        key_name = self.randname()
        rand = self.randint()
        obj = bucket.new(key_name, rand)
        obj.store()
        read1 = bucket.get(key_name)
        read2 = bucket.get(key_name)
        rand1 = self.randint()
        read1.data = rand1
        read1.store()
        read1.reload()
        # read2 will have old VClock so write should fail
        rand2 = self.randint()
        read2.data = rand2
        with self.assertRaises(Exception):
            read2.store()

    def test_consistency_empty_key(self):
        pass
