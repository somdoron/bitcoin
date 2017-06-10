#!/usr/bin/env python3
# Copyright (c) 2015-2016 The Bitcoin Core developers
# Distributed under the MIT software license, see the accompanying
# file COPYING or http://www.opensource.org/licenses/mit-license.php.
"""Test the ZMQ API."""
import configparser
import os
import struct
import time
import logging

from test_framework.test_framework import BitcoinTestFramework, SkipTest
from test_framework.util import *

logging.basicConfig(format='%(message)s', level=logging.DEBUG)

def wait_for_multipart(socket, timeout=60):
    interval = 0.1
    for _ in range(int(timeout/interval)):
        try:
            return socket.recv_multipart(zmq.NOBLOCK)
        except zmq.error.Again:
            time.sleep(interval)

    raise AssertionError("Timed out waiting for zmq message")


class ZMQTest (BitcoinTestFramework):

    def __init__(self):
        super().__init__()
        self.num_nodes = 4

    port = 28332

    def setup_nodes(self):
        # Try to import python3-zmq. Skip this test if the import fails.
        try:
            global zmq
            import zmq
        except ImportError:
            raise SkipTest("python3-zmq module not available.")

        # Check that bitcoin has been built with ZMQ enabled
        config = configparser.ConfigParser()
        if not self.options.configfile:
            self.options.configfile = os.path.dirname(__file__) + "/config.ini"
        config.read_file(open(self.options.configfile))

        if not config["components"].getboolean("ENABLE_ZMQ"):
            raise SkipTest("bitcoind has not been built with zmq enabled.")

        self.zmqContext = zmq.Context()
        self.zmqSubSocket = self.zmqContext.socket(zmq.SUB)
        self.zmqSubSocket.setsockopt(zmq.SUBSCRIBE, b"hashblock")
        self.zmqSubSocket.setsockopt(zmq.SUBSCRIBE, b"hashtx")
        self.zmqSubSocket.setsockopt(zmq.SUBSCRIBE, b"hashwallettx")
        self.zmqSubSocket.connect("tcp://127.0.0.1:%i" % self.port)
        self.nodes = self.start_nodes(self.num_nodes, self.options.tmpdir, extra_args=[
            ['-zmqpubhashtx=tcp://127.0.0.1:'+str(self.port), '-zmqpubhashblock=tcp://127.0.0.1:'+str(self.port),
             '-zmqpubhashwallettx=tcp://127.0.0.1:'+str(self.port)],
            [],
            [],
            []
            ])

    def run_test(self):
        self.sync_all()

        genhashes = self.nodes[0].generate(1)
        self.sync_all()

        self.log.info("1\n")

        self.log.info("listen...")
        msg = wait_for_multipart(self.zmqSubSocket)
        topic = msg[0]
        assert_equal(topic, b"hashtx")
        body = msg[1]
        msgSequence = struct.unpack('<I', msg[-1])[-1]
        assert_equal(msgSequence, 0) #must be sequence 0 on hashtx

        self.log.info("2\n")

        msg = wait_for_multipart(self.zmqSubSocket)
        topic = msg[0]
        assert_equal(topic, b"hashwallettx-block")
        body = msg[1]
        msgSequence = struct.unpack('<I', msg[-1])[-1]
        assert_equal(msgSequence, 0) #must be sequence 0 on hashwallettx

        self.log.info("3\n")

        msg = wait_for_multipart(self.zmqSubSocket)
        topic = msg[0]
        body = msg[1]
        msgSequence = struct.unpack('<I', msg[-1])[-1]
        assert_equal(msgSequence, 0) #must be sequence 0 on hashblock
        blkhash = bytes_to_hex_str(body)

        assert_equal(genhashes[0], blkhash) #blockhash from generate must be equal to the hash received over zmq

        self.log.info("4\n")

        n = 10
        genhashes = self.nodes[1].generate(n)
        self.sync_all()

        zmqHashes = []
        blockcount = 0
        for x in range(0,n*2):
            msg = wait_for_multipart(self.zmqSubSocket)
            self.log.info("5 %d\n" % x)
            topic = msg[0]
            assert_not_equal(topic, b"hashwallettx-block") # as originated from another node must not belong to node0 wallet
            assert_not_equal(topic, b"hashwallettx-mempool")
            body = msg[1]
            if topic == b"hashblock":
                zmqHashes.append(bytes_to_hex_str(body))
                msgSequence = struct.unpack('<I', msg[-1])[-1]
                assert_equal(msgSequence, blockcount+1)
                blockcount += 1

        self.log.info("6\n")

        for x in range(0,n):
            assert_equal(genhashes[x], zmqHashes[x]) #blockhash from generate must be equal to the hash received over zmq

        #test tx from a second node
        hashRPC = self.nodes[1].sendtoaddress(self.nodes[0].getnewaddress(), 1.0)
        self.sync_all()

        self.log.info("7\n")

        # now we should receive a zmq msg because the tx was broadcast
        msg = wait_for_multipart(self.zmqSubSocket)
        topic = msg[0]
        body = msg[1]
        hashZMQ = ""
        if topic == b"hashtx":
            hashZMQ = bytes_to_hex_str(body)
            msgSequence = struct.unpack('<I', msg[-1])[-1]
            assert_equal(msgSequence, blockcount+1)

        assert_equal(hashRPC, hashZMQ) #blockhash from generate must be equal to the hash received over zmq

        self.log.info("8\n")

        msg = wait_for_multipart(self.zmqSubSocket)
        topic = msg[0]
        assert_equal(topic, b"hashwallettx-mempool")
        body = msg[1]
        hashZMQ = bytes_to_hex_str(body)
        msgSequence = struct.unpack('<I', msg[-1])[-1]

        assert_equal(msgSequence, 1)
        assert_equal(hashRPC, hashZMQ)

        self.log.info("9\n")

        # Destroy the zmq context
        self.zmqContext.destroy(linger=None)

        self.log.info("10\n")

if __name__ == '__main__':
    ZMQTest ().main ()
