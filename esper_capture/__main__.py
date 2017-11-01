# ESPER Waveform and Variable viewer

from __future__ import print_function
from builtins import str as text

import threading
import os
import sys
import argparse
import cmd
import time
import getpass
import platform
import configparser
import zmq
import socket
import struct
import signal
import re
import queue
import pyqtgraph as pg
from pyqtgraph.Qt import QtCore, QtGui
import numpy as np
import zlib
import msgpack
import time
from .version import __version__

if(platform.system() == u'Windows'):
    import ctypes
    import pyreadline as readline
else:
    import readline

here = os.path.abspath(os.path.dirname(__file__))

version = __version__

MTU_SIZE = 1500
        

def getAuth(args, config):
    username = False
    password = False

    if(args.user):
        username = args.user
        if(not args.password):
            password = getpass.getpass("Password for " + username + ": ") 
        else:
            password = args.password
    
    else:
        if(config.has_section('auth')):
            if(config.has_option('auth','username')):
                username = config.get('auth','username')

            if(username):
                if(config.has_option('auth','password')):
                    password = config.get('auth','password')
                else:
                    password = getpass.getpass("Password for " + username + ": ") 
        else:
            # Config file has no value yet, might as well use the one passed in (or the default)
            config.add_section('auth')
            config.add_option('username', username)

    return { 'username' : username, 'password' : password }

def send_zipped_pickle(socket, obj, flags=0, protocol=-1):
    """pickle an object, and zip the pickle before sending it"""
    p = pickle.dumps(obj, protocol)
    z = zlib.compress(p)
    return socket.send(z, flags=flags)

def send_msgpack(socket, obj, flags=0, protocol=-1):
    """pickle an object, and zip the pickle before sending it"""
    p = msgpack.packb(obj, use_bin_type=True)
    return socket.send(p, flags=flags)


def getData(sock, q):

    sample_count = 0
    trigger_start = 0
    trigger_delay = 0
    waveform = [0] * (76*4*511) #np.zeros((76*4*511), np.dtype('i2'))
    waveform_len = 0

    last_msg_count = False
    last_chunk_num = 0
    in_msg = False

    while(True):        
        try: 
            data, addr = sock.recvfrom(MTU_SIZE)

            if(data):
                # Discard if we aren't getting the first packet
                msg_count   = struct.unpack_from("<I",data,0)[0] # Overall message count
                chunk_num   = struct.unpack_from("<H",data,4)[0] # Chunk number, should start at zero.
                sample_cnt  = struct.unpack_from("<H",data,6)[0] # Samples captured
                sample_byte_cnt = struct.unpack_from("<H",data,8)[0] # waveform bytes sent in this packet

                if(chunk_num == 0):
                    in_msg = True
                    waveform_len = 0
                    trigger_start = struct.unpack_from("<Q",data,10)[0]
                    triger_delay = struct.unpack_from("<Q",data,18)[0] - trigger_start
                    
                if(in_msg):
                    if(last_msg_count == False):
                        last_msg_count = msg_count
                        if(chunk_num != 0):
                            last_chunk_num = chunk_num - 1    
                        else:
                            last_chunk_num = chunk_num

                    if(last_msg_count == msg_count):
                        if(chunk_num != 0):
                            if(last_chunk_num != (chunk_num - 1)):
                                print("Missed " + str(chunk_num - 1 - last_chunk_num) + " Chunk" + " " + str(last_chunk_num) + " " + str(chunk_num))                        
                                in_msg = False # Bail out of being in a message until the next chunk_num == 0 comes around
                    else:
                        if(last_msg_count != msg_count - 1):
                            print("Missed " + str(msg_count - 1 - last_msg_count) + " Messages")    
                                                                    
                # Still a valid message? Let's put together the waveform data 
                if(in_msg):

                    if(chunk_num == 0):
                        offset = 26
                    else:
                        offset = 10

                    for n in range(0, sample_byte_cnt, 2):        
                        waveform[waveform_len] = struct.unpack_from("<h",data,int(offset+n))[0]
                        waveform_len = waveform_len + 1
                                            
                    if(chunk_num == 255):
                        # lets add it to the queue! 
                        if((in_msg == True) and (waveform_len == 155344)):
                            print("Message " + str(last_msg_count) + " Received")
                            q.put({ 
                                u'msg_count': msg_count, 
                                u'chunk_num': chunk_num, 
                                u'waveform_len': waveform_len,
                                u'waveform': waveform })
                        else:
                            print("Message Error has occurred. Possible length mismatch? Waveform length: " + str(msg.waveform_len))


                last_msg_count = msg_count
                last_chunk_num = chunk_num
                
        except BlockingIOError:
            pass

def putData(pub, q):
    # whenever the queue has data, we will pass it along to 0MQ
    # in the future this will be a separate thread to de-sync the UDP and the zeromq 
    while True:
        while not q.empty():
            pub.send(msgpack.packb(q.get(), use_bin_type=False))
            
        time.sleep(1)

def main():
    try:
        prog='esper-capture'    

        parser = argparse.ArgumentParser(prog=prog)

        # Verbose, because sometimes you want feedback
        parser.add_argument('-v','--verbose', help="Verbose output", default=False, action='store_true')
        parser.add_argument('--version', action='version', version='%(prog)s ' + version)

        parser.add_argument("-f", "--config", default="test.ini", help="Config file for node")
        parser.add_argument("-s", "--storage", default="", help="Storage path for collected data")
        parser.add_argument("-u", "--user", default=False, help="User for Auth")
        parser.add_argument("-p", "--password", default=False, help="Password for Auth")
        parser.add_argument("ip", help="IP address of node to pull data from")
        parser.add_argument("pub", help="IP address of ZeroMQ Publisher")
        #parser.add_argument("port", type=int, default=50005, help="Port of node to pull data from")

        # Put the arguments passed into args
        args = parser.parse_args()

        try: 
            # Load up config
            # Create config instance
            config = configparser.SafeConfigParser()

            # Load configuration file
            config.read(args.config)
        
            auth = getAuth(args, config)

            # if a username has been defined, then a password *MUST* have been grabbed, perform authentication
            if(auth['username']):
                print(auth['username'] + ' ' + auth['password'])

            addr = re.split(' :', args.ip)
            if(len(addr) < 2):
                addr.append( 50005 )

            # Attempt to gather data from UDP source
            udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            udp_sock.bind((addr[0], addr[1]))

            # Setup 0MQ publisher
            addr = re.split(' :', args.pub)
            if(len(addr) < 2):
                addr.append( 50006 )

            context = zmq.Context()
            pub = context.socket(zmq.PUB)
            pub.bind("tcp://"+str(addr[0])+":"+str(addr[1]))

            # Create Queue
            q = queue.Queue()

            thread_udp = threading.Thread(target=getData, args=(udp_sock, q,))
            thread_udp.setDaemon(True)
            thread_udp.start()
            thread_pub = threading.Thread(target=putData, args=(pub, q,))
            thread_pub.setDaemon(True)
            thread_pub.start()

            while(1):
                #print("Current Queue Size: " + str(q.qsize()))
                if(q.full()):
                    print("Queue is full!")
                time.sleep(1)

            # No options selected, this should never be reached
            sys.exit(0) 

        except Exception as err:
            print(err)
            sys.exit(1)
              
    except KeyboardInterrupt:
        print("\nExiting " + prog)
        udp_sock.close()

        sys.exit(0)

if __name__ == "__main__":
    main()