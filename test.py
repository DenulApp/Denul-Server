#!/usr/bin/env python2

import socket
import struct
import ssl
import zlib

from bitstring import ConstBitStream
from hashlib import sha256
from os import urandom

from messages.c2s_pb2 import ClientHello, ServerHello, Store, StoreReply
from messages.metaMessage_pb2 import Wrapper
from vicbf.vicbf import deserialize

# This file contains test cases for the server application.
# It assumes the server is already running on the standard port of 5566, with
# the server.key and server.crt in the same directory.
# This test suite relies on the tests being executed __in order__. Nosetests
# does this by default, but your mileage may vary if you use other test suites.
#
# The test cases will also generate a number of database entries on the server,
# and will not necessarily remove them all afterwards.
# Keep this in mind when running it against a production server.


def printMsg(msg):
    assert len(msg) <= 74
    print msg, " " * (74 - len(msg)),


def getSocket():
    tsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sslsock = ssl.wrap_socket(tsock,
                              ca_certs="server.crt",
                              cert_reqs=ssl.CERT_REQUIRED)
    sslsock.connect(("127.0.0.1", 5566))
    return sslsock


def SocketReadN(sock, n):
    buf = b''
    while n > 0:
        data = sock.recv(n)
        if data == b'':
            raise RuntimeError('unexpected connection close')
        buf += data
        n -= len(data)
    return buf


def RecvOneMsg(sock):
    try:
        lengthbuf = SocketReadN(sock, 4)
        length = struct.unpack(">i", lengthbuf)[0]
        wrapper = Wrapper()
        wrapper.ParseFromString(SocketReadN(sock, length))
        return wrapper
    except Exception, e:
        print e
        return None


def sendOneMsg(msg, sock):
    mm = msg.SerializeToString()
    sock.sendall(struct.pack(">i", len(mm)) + mm)


def transceive(msg, sock):
    sendOneMsg(msg, sock)
    return RecvOneMsg(sock)


def getClientHelloMessage(version="1.0"):
    ch = ClientHello()
    ch.clientProto = version
    wrapper = Wrapper()
    wrapper.ClientHello.MergeFrom(ch)
    return wrapper


def getStoreMessage(key, value):
    st = Store()
    st.key = key
    st.value = value
    wrapper = Wrapper()
    wrapper.Store.MergeFrom(st)
    return wrapper


def parseVICBF(serialized):
    decomp = zlib.decompress(serialized)
    bs = ConstBitStream(bytes=decomp)
    return deserialize(bs)


##### Test Suite
### ClientHello Test
def test_ClientHello():
    # This test sends a valid ClientHello and ensures that the reply is valid
    # and contains a valid compressed VICBF
    sock1 = getSocket()
    msg = getClientHelloMessage()
    reply = transceive(msg, sock1)
    assert reply.WhichOneof('message') == 'ServerHello'
    assert reply.ServerHello.opcode == ServerHello.CLIENT_HELLO_OK
    assert reply.ServerHello.serverProto == "1.0"
    assert reply.ServerHello.data != b'0'
    try:
        assert parseVICBF(reply.ServerHello.data) is not None
    except:
        assert False, "Unexpected exception occured during VICBF parsing"
    sock1.close()


def test_ClientHello_invalid():
    # This test sends in invalid ClientHello and ensures that the reply is an
    # error message
    sock1 = getSocket()
    msg = getClientHelloMessage(version="2.0")
    reply = transceive(msg, sock1)
    assert reply.WhichOneof('message') == 'ServerHello'
    assert reply.ServerHello.opcode == \
        ServerHello.CLIENT_HELLO_PROTO_NOT_SUPPORTED
    assert reply.ServerHello.serverProto == "1.0"
    assert reply.ServerHello.data == b'0'
    sock1.close()


def test_Store():
    # This test attempts to store a key-value-pair on the server
    sock1 = getSocket()
    nonce = urandom(8)
    value = urandom(16).encode('hex')
    rev = sha256(nonce).hexdigest()
    key = sha256(rev).hexdigest()
    msg = getStoreMessage(key, value)
    reply = transceive(msg, sock1)
    assert reply.WhichOneof('message') == 'StoreReply'
    assert reply.StoreReply.opcode == StoreReply.STORE_OK
    assert reply.StoreReply.key == key
    sock1.close()


def test_Store_retr_VICBF():
    # This test attempts to store a key-value-pair on the server and checks
    # if the key has been inserted into the VICBF
    # Insert KV on server
    sock1 = getSocket()
    nonce = urandom(8)
    value = urandom(16).encode('hex')
    rev = sha256(nonce).hexdigest()
    key = sha256(rev).hexdigest()
    msg = getStoreMessage(key, value)
    reply = transceive(msg, sock1)
    assert reply.WhichOneof('message') == 'StoreReply'
    assert reply.StoreReply.opcode == StoreReply.STORE_OK
    assert reply.StoreReply.key == key
    # KV has been inserted
    # Get a ServerHello message with the updated VICBF from the server
    msg = getClientHelloMessage()
    reply = transceive(msg, sock1)
    # Ensure that we got a good ServerHello
    assert reply.WhichOneof('message') == 'ServerHello'
    assert reply.ServerHello.opcode == ServerHello.CLIENT_HELLO_OK
    # Parse VICBF
    v = parseVICBF(reply.ServerHello.data)
    # Assert VICBF status
    assert key in v
    assert rev not in v
    sock1.close()
