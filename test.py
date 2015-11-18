#!/usr/bin/env python2

import socket
import struct
import ssl

from os import urandom

from messages.c2s_pb2 import ClientHello, ServerHello
from messages.metaMessage_pb2 import Wrapper


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

### ClientHello Test
sock1 = getSocket()
printMsg('Testing valid ClientHello...')
msg = getClientHelloMessage()
reply = transceive(msg, sock1)
assert reply.WhichOneof('message') == 'ServerHello'
assert reply.ServerHello.opcode == ServerHello.CLIENT_HELLO_OK
assert reply.ServerHello.serverProto == "1.0"
print "[OK]"

printMsg('Testing invalid ClientHello...')
msg = getClientHelloMessage(version="2.0")
reply = transceive(msg, sock1)
assert reply.WhichOneof('message') == 'ServerHello'
assert reply.ServerHello.opcode == ServerHello.CLIENT_HELLO_PROTO_NOT_SUPPORTED
assert reply.ServerHello.serverProto == "1.0"
print "[OK]"
sock1.close()
