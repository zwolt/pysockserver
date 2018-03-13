#!/usr/bin/env python
# -*- coding: utf8 -*-


import logging
import re,sys,os

import SocketServer

import settings,socket,subprocess

from signal import SIGTERM, SIGCHLD, signal, alarm
from threading import Thread
from time import *
from smdr_lib import *

'''
class SingleTCPHandler(SocketServer.BaseRequestHandler):
    "One instance per connection.  Override handle(self) to customize action."
    def handle(self):
        alarm(30)
        data = self.request.recv(1024) 
        # Some code
        self.request.close()
'''
class ServHandler(SocketServer.ForkingMixIn, SocketServer.TCPServer):
    def __init__(self, server_address, RequestHandlerClass):
        #return RequestHandlerClass.state()
        #print 'start handle'
        #daemon_threads = True
        #allow_reuse_address = True
        SocketServer.TCPServer.__init__(self, server_address, RequestHandlerClass)
    
    def deamonize(self, stdout='/dev/null', stderr=None, stdin='/dev/null', pidfile='pid.txt', startmsg='started with pid %s'):
        
        try:
            pid = os.fork()
            if (pid > 0):
                sys.exit(0)
        except OSError, e:
            sys.stderr.write("fork #1 failed: (%d) %s\n" % (e.errno, e.strerror))
            sys.exit(1)
        
        os.chdir(settings.place)
        os.umask(0)
        os.setsid()
        
        try:
            pid = os.fork()
            if (pid > 0):
                sys.exit(0) 
        except OSError, e:
            sys.stderr.write("fork #2 failed: (%d) %s\n" % (e.errno, e.strerror))
            sys.exit(1)
        
        sys.stdout.flush()
        sys.stderr.flush()
        if (not stderr):
            stderr = stdout
            si = open(stdin, 'r')
            so = open(stdout, 'a+')
            se = open(stderr, 'a+', 0)
            pid = str(os.getpid())
            #sys.stdout.write("\n%s\n" % startmsg % pid)
            #sys.stdout.flush()
        if pidfile: file(pidfile, 'w+').write("%s\n" % pid)
        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())
        print("%s" % startmsg % pid)


class Server():
    def startstop(self, stdout='/dev/null', stderr=None, stdin='/dev/null', pidfile='pid.txt', startmsg='started with pid %s'):
        if len(sys.argv) > 1:
            action = sys.argv[1]
            #mess = ""
            try:
                pf = open(pidfile)
                pid = int(pf.read().strip())
                pf.close()
            except IOError:
                pid = None
            if ((action == 'stop') or (action == 'restart')):
                if (not pid):
                    mess = "Не могу остановить, pid файл '%s' отсутствует.\n"
                    sys.stderr.write(mess % pidfile)
                    sys.exit(1)
                try:
                    while 1:
                        os.kill(pid, SIGTERM)
                        sleep(1)
                except OSError, err:
                    err = str(err)
                    if err.find("No such process") > 0:
                        os.remove(pidfile)
                        if 'stop' == action:
                            sys.exit(0)
                        action = 'start'
                        pid = None
                    else:
                        sys.exit(1)
                        print str(err)
            if ('start' == action):
                if (pid):
                    mess = "Старт отменен — pid файл '%s' существует.\n"
                    sys.stderr.write(mess % pidfile)
                    sys.exit(1)
                srv=ServHandler((settings.host,settings.port),RecvHandler)
                srv.deamonize(stdout, stderr, stdin, pidfile, startmsg)
                srv.serve_forever()
                #sys.stderr.write(mess % pidfile)
                return
        print "Синтакс запуска: %s start|stop|restart" % sys.argv[0]
        sys.exit(2)

if (__name__ == "__main__"):
    Server().startstop(stdout=settings.log, pidfile=settings.pid)
