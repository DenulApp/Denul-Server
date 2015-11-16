# -*- encoding: utf-8 -*-
#!/usr/bin/env python

# This code is adapted from the NFCGate Server Code
# https://github.com/nfcgate/server
# The NFCGate Server is licensed under the Apache License v2

import select
import socket
import ssl
import struct

from messages import c2s_pb2
from messages import metaMessage_pb2

HOST = "0.0.0.0"
PORT = 5566


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
    wrapper = metaMessage_pb2.Wrapper()
    wrapper.ParseFromString(SocketReadN(sock, length))
    return wrapper


def sendMessage(msg, sock):
    ms = msg.SerializeToString()
    # mb = [elem.encode('hex') for elem in ms]
    # Messages are sent as byte strings prefixed with their own length
    sock.sendall(struct.pack(">i", len(ms)) + ms)


### Debugging helper functions
def prettyPrintProtobuf(msg, sock):
    pass  # TODO Reimplement


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


# Handler for all incoming messages
def HandleMessage(message, sock):
    mtype = message.WhichOneof('message')
    if mtype == "Store":
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

    print "Denul server started on port " + str(PORT)

    try:
        while 1:
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

    # If we reach this statement, the main loop has terminated
    # Close the socket
    server_socket.close()
