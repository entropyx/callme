# Copyright (c) 2009-2014, Christian Haintz
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met:
#
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#
#     * Redistributions in binary form must reproduce the above
#       copyright notice, this list of conditions and the following
#       disclaimer in the documentation and/or other materials provided
#       with the distribution.
#
#     * Neither the name of callme nor the names of its contributors
#       may be used to endorse or promote products derived from this
#       software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
# A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
# OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
# THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
import threading
import time

import callme
from callme import exceptions as exc
from callme import test

class ActionsTestCase(test.TestCase):


    @staticmethod
    def _run_server_thread(server):
        t = threading.Thread(target=server.start)
        t.daemon = True
        t.start()
        server.wait()
        return t

    def test_method_single_call(self):
        server = callme.Server(exchange_name="foo_ex", queue_name="foo.receive")
        server.register_function(lambda a, b: a + b, 'madd')
        p = self._run_server_thread(server)
        # server_exchange_name ="foo_ex", server_queue_name="foo.receive"
        try:
            result = callme.Proxy(server_exchange_name ="foo_ex", server_queue_name="foo.receive").madd(1, 1)
            self.assertEqual(result, 2)
        finally:
            server.stop()
        p.join()

    def test_method_multiple_calls(self):
            server = callme.Server(exchange_name="foo_ex", queue_name="foo.receive")
            server.register_function(lambda a, b: a + b, 'madd')
            p = self._run_server_thread(server)

            try:
                proxy = callme.Proxy(server_exchange_name ="foo_ex", server_queue_name="foo.receive")

                result = proxy.use_server(timeout=3).madd(1, 2)
                self.assertEqual(result, 3)

                result = proxy.use_server(timeout=2).madd(2, 2)
                self.assertEqual(result, 4)

                result = proxy.use_server(timeout=1).madd(2, 3)
                self.assertEqual(result, 5)
            finally:
                server.stop()
            p.join()

    def test_serial_server_concurrent_calls(self):

        def madd(a):
            time.sleep(0.1)
            return a

        server = callme.Server(exchange_name="foo_ex", queue_name="foo.receive")
        server.register_function(madd, 'madd')
        p = self._run_server_thread(server)

        def threaded_call(i, results):
            proxy = callme.Proxy(server_exchange_name ="foo_ex", server_queue_name="foo.receive")
            results.append((i, proxy.madd(i)))

        results = []
        threads = []
        try:
            # start 5 threads who call "parallel"
            for i in range(5):
                t = threading.Thread(target=threaded_call, args=(i, results))
                t.daemon = True
                t.start()
                threads.append(t)

            # wait until all threads are finished
            [thread.join() for thread in threads]
        finally:
            server.stop()
        p.join()

        # check results
        for i, result in results:
            self.assertEqual(i, result)

    def test_threaded_server_concurrent_calls(self):

        def madd(a):
            time.sleep(0.1)
            return a

        server = callme.Server(exchange_name="foo_ex", queue_name="foo.receive", threaded=True)
        server.register_function(madd, 'madd')
        p = self._run_server_thread(server)

        def threaded_call(i, results):
            results.append((i, callme.Proxy(server_exchange_name ="foo_ex", server_queue_name="foo.receive").madd(i)))

        results = []
        threads = []
        try:
            # start 5 threads who call "parallel"
            for i in range(5):
                t = threading.Thread(target=threaded_call, args=(i, results))
                t.daemon = True
                t.start()
                threads.append(t)

            # wait until all threads are finished
            [thread.join() for thread in threads]
        finally:
            server.stop()
        p.join()

        # check results
        for i, result in results:
            self.assertEqual(i, result)

    def test_timeout_call(self):
        callme.Server(exchange_name="foo_ex", queue_name="foo.receive")
        proxy = callme.Proxy(server_exchange_name ="foo_ex", server_queue_name="foo.receive", timeout=1)
        self.assertRaises(exc.RpcTimeout, proxy.madd, 1, 2)

    def test_remote_exception_call(self):
        server = callme.Server(exchange_name="foo_ex", queue_name="foo.receive")
        server.register_function(lambda a, b: a + b, 'madd')
        p = self._run_server_thread(server)

        try:
            proxy = callme.Proxy(server_exchange_name ="foo_ex", server_queue_name="foo.receive")

            self.assertRaises(TypeError, proxy.madd)
        finally:
            server.stop()
        p.join()
