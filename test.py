#!/usr/bin/env python2

import socket
import struct
import ssl
import zlib
from bitstring import ConstBitStream

from messages.c2s_pb2 import ClientHello, ServerHello
from messages.metaMessage_pb2 import Wrapper
from vicbf.vicbf import VICBF, deserialize


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
    decomp = zlib.decompress(reply.ServerHello.data)
    bs = ConstBitStream(bytes=decomp)
    deserialize(bs)
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
