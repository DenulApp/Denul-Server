#!/usr/bin/env python2

import socket
import struct
import ssl
import zlib

from bitstring import ConstBitStream
from hashlib import sha256
from os import urandom

from messages.c2s_pb2 import ClientHello, ServerHello, Store, StoreReply, \
    Delete, DeleteReply
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


def getDeleteMessage(key, auth):
    dl = Delete()
    dl.key = key
    dl.auth = auth
    wrapper = Wrapper()
    wrapper.Delete.MergeFrom(dl)
    return wrapper


def getKVPair():
    nonce = urandom(8)
    value = urandom(16).encode('hex')
    auth = sha256(nonce).hexdigest()
    key = sha256(auth).hexdigest()
    return key, auth, value


def parseVICBF(serialized):
    decomp = zlib.decompress(serialized)
    bs = ConstBitStream(bytes=decomp)
    return deserialize(bs)


def assertServerHelloState(msg, opcode=ServerHello.CLIENT_HELLO_OK):
    assert msg.WhichOneof('message') == 'ServerHello'
    assert msg.ServerHello.opcode == opcode
    assert msg.ServerHello.serverProto == "1.0"


def assertStoreState(msg, key, opcode=StoreReply.STORE_OK):
    assert msg.WhichOneof('message') == 'StoreReply'
    assert msg.StoreReply.opcode == opcode
    assert msg.StoreReply.key == key


def assertDeletionState(msg, key, opcode=DeleteReply.DELETE_OK):
    assert msg.WhichOneof('message') == 'DeleteReply'
    assert msg.DeleteReply.opcode == opcode
    assert msg.DeleteReply.key == key


def delete(key, auth, sock):
    msg = getDeleteMessage(key, auth)
    reply = transceive(msg, sock)
    assertDeletionState(reply, key)


def store(key, value, sock):
    msg = getStoreMessage(key, value)
    reply = transceive(msg, sock)
    assertStoreState(reply, key)


##### Test Suite
def test_ClientHello():
    # This test sends a valid ClientHello and ensures that the reply is valid
    # and contains a valid compressed VICBF
    sock = getSocket()
    msg = getClientHelloMessage()
    reply = transceive(msg, sock)
    assertServerHelloState(reply)
    assert reply.ServerHello.data != b'0'
    try:
        assert parseVICBF(reply.ServerHello.data) is not None
    except:
        assert False, "Unexpected exception occured during VICBF parsing"
    sock.close()


def test_ClientHello_invalid():
    # This test sends in invalid ClientHello and ensures that the reply is an
    # error message
    sock = getSocket()
    msg = getClientHelloMessage(version="2.0")
    reply = transceive(msg, sock)
    assert reply.WhichOneof('message') == 'ServerHello'
    assertServerHelloState(reply, ServerHello.CLIENT_HELLO_PROTO_NOT_SUPPORTED)
    assert reply.ServerHello.data == b'0'
    sock.close()


def test_Store_and_Delete():
    # This test attempts to store a key-value-pair on the server
    sock = getSocket()
    key, auth, value = getKVPair()
    store(key, value, sock)
    msg = getDeleteMessage(key, auth)
    reply = transceive(msg, sock)
    assertDeletionState(reply, key)
    sock.close()


def test_Store_in_VICBF():
    # This test attempts to store a key-value-pair on the server and checks
    # if the key has been inserted into the VICBF
    # Insert KV on server
    sock = getSocket()
    key, auth, value = getKVPair()
    store(key, value, sock)
    # KV has been inserted
    # Get a ServerHello message with the updated VICBF from the server
    msg = getClientHelloMessage()
    reply = transceive(msg, sock)
    # Ensure that we got a good ServerHello
    assertServerHelloState(reply)
    # Parse VICBF
    v = parseVICBF(reply.ServerHello.data)
    # Assert VICBF status
    assert key in v
    assert auth not in v
    # Delete kv pair from server
    delete(key, auth, sock)
    sock.close()


def test_Store_invalid_key_length():
    # This test checks the behaviour of the server if we are trying to store
    # a key with the wrong length
    sock = getSocket()
    key = "deadbeefdecafbad"
    msg = getStoreMessage(key, key)
    reply = transceive(msg, sock)
    assertStoreState(reply, key, opcode=StoreReply.STORE_FAIL_KEY_FMT)
    sock.close()


def test_Store_invalid_key_characters():
    # This test checks the behaviour of the server if we are trying to store
    # a key with non-hexadecimal characters
    sock = getSocket()
    key = "x" * 64
    msg = getStoreMessage(key, key)
    reply = transceive(msg, sock)
    assertStoreState(reply, key, opcode=StoreReply.STORE_FAIL_KEY_FMT)
    sock.close()


def test_Store_duplicate_key():
    # This test checks the behaviour of the server if we are trying to store
    # two values under the same key
    sock = getSocket()
    key, auth, value = getKVPair()
    store(key, value, sock)
    value = urandom(16).encode('hex')
    msg = getStoreMessage(key, value)
    reply = transceive(msg, sock)
    assertStoreState(reply, key, opcode=StoreReply.STORE_FAIL_KEY_TAKEN)
    delete(key, auth, sock)
    sock.close()


def test_Delete_bad_auth():
    # Test if the deletion really fails if we use a bad authenticator
    sock = getSocket()
    key, auth, value = getKVPair()
    store(key, value, sock)
    msg = getDeleteMessage(key, key)
    reply = transceive(msg, sock)
    assertDeletionState(reply, key, opcode=DeleteReply.DELETE_FAIL_AUTH)
    delete(key, auth, sock)


def test_Delete_bad_key():
    # Test if the deletion fails if a bad key is provided
    sock = getSocket()
    key, auth, value = getKVPair()
    store(key, value, sock)
    msg = getDeleteMessage("x" * 64, "x" * 64)
    reply = transceive(msg, sock)
    assertDeletionState(reply, "x" * 64, opcode=DeleteReply.DELETE_FAIL_KEY_FMT)
    delete(key, auth, sock)


def test_Delete_nonexistant_key():
    # Test if the deletion fails if the key is not present
    sock = getSocket()
    key, auth, value = getKVPair()
    msg = getDeleteMessage(key, auth)
    reply = transceive(msg, sock)
    assertDeletionState(reply, key, opcode=DeleteReply.DELETE_FAIL_NOT_FOUND)
