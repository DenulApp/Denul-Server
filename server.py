# -*- encoding: utf-8 -*-
#!/usr/bin/env python2

# This code is adapted from the NFCGate Server Code
# https://github.com/nfcgate/server
# The NFCGate Server is licensed under the Apache License v2
#
# The NFCGate server code code was in turn inspired by
# http://www.binarytides.com/code-chat-application-server-client-sockets-python

import select
import socket
import ssl
import struct
import sys
import zlib

from messages.c2s_pb2 import ClientHello, ServerHello
from messages.metaMessage_pb2 import Wrapper

from storage.sqlite import SqliteBackend

from vicbf.vicbf import VICBF


class Cache():
    def __init__(self):
        self.vicbfcache = None

    def getVicbfCache(self):
        if self.vicbfcache is not None:
            return self.vicbfcache
        else:
            serialized = VicbfBackend.serialize().tobytes()
            self.vicbfcache = zlib.compress(serialized, 6)
            return self.vicbfcache

    def invalidateVicbf(self):
        self.vicbfcache = None


HOST = "0.0.0.0"
PORT = 5566

DEBUG = True

DatabaseBackend = None

VicbfBackend = None
VicbfCache = Cache()

THRESH_UP = None


### Logging helper functions
def debug(string):
    """Print a debugging string if debugging is active.

    Displays the function name of the caller to make debugging easier.
    Caller detection code adapted from:
    http://jugad2.blogspot.in/2015/09/find-caller-and-callers-caller-of.html
    """
    if DEBUG:
        print sys._getframe(1).f_code.co_name + ":", string


### Network helper functions
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
    # Messages are prefixed with a 4-byte length indicator
    lengthbuf = SocketReadN(sock, 4)
    length = struct.unpack('>i', lengthbuf)[0]
    wrapper = Wrapper()
    wrapper.ParseFromString(SocketReadN(sock, length))
    return wrapper


def sendMessage(msg, sock):
    ms = msg.SerializeToString()
    # mb = [elem.encode('hex') for elem in ms]
    # Messages are sent as byte strings prefixed with their own length
    sock.sendall(struct.pack(">i", len(ms)) + ms)
    debug("Message sent")


### Debugging helper functions
def prettyPrintProtobuf(msg, sock):
    pass  # TODO Reimplement


### Helper function for the VICBF
def getVicbfSerialization():
    return VicbfCache.getVicbfCache()


def invalidateVicbfSerializationCache():
    VicbfCache.invalidateVicbf()


##### Message Creation Functions
# Example function:
# def getSessionMessage(code_tuple):
#     imsg = c2s_pb2.Session()
#     imsg.opcode = code_tuple[0]
#     imsg.errcode = code_tuple[1]
#     msg = metaMessage_pb2.Wrapper()
#     msg.Session.MergeFrom(imsg)
#     return msg


##### Handlers
# Handler for "Store" messages
def HandleStoreMessage(msg, sock):
    pass


def HandleClientHelloMessage(msg, sock):
    rv = ServerHello()
    rv.serverProto = "1.0"
    if msg.clientProto == "1.0":
        debug("Valid clientProto received")
        rv.opcode = ServerHello.CLIENT_HELLO_OK
        rv.data = getVicbfSerialization()
    else:
        debug("WARN: Invalid clientProto received")
        rv.opcode = ServerHello.CLIENT_HELLO_PROTO_NOT_SUPPORTED
        rv.data = b'0'
    wrapper = Wrapper()
    wrapper.ServerHello.MergeFrom(rv)
    debug("Returning...")
    return wrapper


# Handler for all incoming messages
def HandleMessage(message, sock):
    mtype = message.WhichOneof('message')
    if mtype == "ClientHello":
        debug("Received ClientHello")
        return HandleClientHelloMessage(message.ClientHello, sock)
    elif mtype == "Store":
        debug("Received Store")
        return HandleStoreMessage(message.Store, sock)
    # and so on


##### Main code
if __name__ == "__main__":

    CONNECTION_LIST = []  # list of socket clients
    RECV_BUFFER = 4096    # Advisable to keep it as an exponent of 2

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    server_socket.bind((HOST, PORT))
    server_socket.listen(10)

    # Add server socket to the list of readable connections
    CONNECTION_LIST.append(server_socket)

    # Prepare the database
    print "Initialize database"
    DatabaseBackend = SqliteBackend()

    print "Read existing keys into VICBF"
    # Read existing keys from the database
    keys = DatabaseBackend.all_keys()
    # Calculate the number of expected entries
    # For now, we will expect the number of entries to double, and add 1000
    # to the estimation to account for very small initial values.
    # This can probably be heavily optimized
    expected_entries = len(keys) * 2 + 1000
    # Taking 10 times the number of expected entries for the slot count will
    # result in a FPR of p = ~0.0007, or 0.07% once the number of expected
    # entries is reached.
    slots = expected_entries * 10
    # Calculate the threshold at which we should generate a new VICBF.
    # After having inserted double the expected entries in the VICBF, the FPR
    # will be at roughly p = 0.006, or 0.6%. At this point, we should generate
    # a new, larger VICBF to accomodate further entries
    THRESH_UP = expected_entries * 2
    # Initialize the VICBF with the given values
    VicbfBackend = VICBF(slots, 3)
    # Insert all existing keys into the VICBF
    for key in DatabaseBackend.all_keys():
        VicbfBackend += key
    # Since nothing time-critical is happening right now, we can take the time
    # to populate the VICBF serialization cache. It is guaranteed to be needed
    # at least once before becoming outdated, as it will be accessed on every
    # new connection. The following call will request the VICBF serialization,
    # which will be cached, and ignore the result.
    print "Populate cache"
    getVicbfSerialization()

    print "Denul server started on port " + str(PORT)

    try:
        while True:
            # Get the list sockets which are ready to be read through select
            read_sockets, write_sockets, error_sockets = \
                select.select(CONNECTION_LIST, [], [])

            for sock in read_sockets:

                # New connection
                if sock == server_socket:
                    # Handle the case in which there is a new connection
                    # recieved through server_socket
                    sockfd, addr = server_socket.accept()

                    # Wrap the socket in a SSL/TLS socket
                    socktls = ssl.wrap_socket(sockfd, server_side=True,
                                              certfile="server.crt",
                                              keyfile="server.key")
                    # I'd love to make this a more secure instance of an SSL
                    # socket, but sadly, this would require python 2.7.9+,
                    # which is not yet available in the ubuntu repos I am
                    # using.
                    # Right now, the socket still allows SSLv3 and RC4
                    # connections, which is horrible, but the alternative
                    # would be to only allow TLSv1 (and not v1.1 / v1.2),
                    # which would be bad form as well.
                    # Once a newer version of python is widely available, I may
                    # change the code to use an ssl.Context object with the
                    # correct settings for a secure socket.
                    CONNECTION_LIST.append(socktls)

                    print "Client (%s, %s) connected" % addr

                # Some incoming message from a client
                else:
                    # Data recieved from client, process it
                    try:
                        wrapperMsg = RecvOneMsg(sock)
                        if wrapperMsg:
                            reply = HandleMessage(wrapperMsg, sock)
                            # w_reply = wrapMessage(reply)
                            sendMessage(reply, sock)

                    # client disconnected, so remove from socket list
                    except Exception, e:
                        print "Client (%s, %s) is offline" % addr
                        sock.close()
                        CONNECTION_LIST.remove(sock)
                        continue

    # Catch KeyboardInterrupts to save state before exiting
    except KeyboardInterrupt:
        print "Interrupted. exiting"

    # Try to close all sockets, ignoring any errors
    for sock in CONNECTION_LIST:
        try:
            sock.close()
        except Exception:
            continue

    # If we reach this statement, the main loop has terminated
    # Close the socket
    server_socket.close()
