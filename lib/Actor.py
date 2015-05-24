import os
import gevent
import gevent.event
import gevent.pool
import zmq.green as zmq
import traceback
import time
from Utils import *
import random
import syslog

class Actor( gevent.Greenlet ):

    # Available to the Actor implementation:
    #  stopEvent, an Event indicating the Actor should stop cleanly
    #  config, json configuration
    #  init, function to define to initialize the Actor
    #  deinit, function to define to cleanup the Actor
    #  handle, set the function to call on messages received on a socket
    #  schedule, set a recurring function

    class _ActorHandle ( object ):
        _zHostDir = None

        def __init__( self, realm, category, mode = 'random' ):
            self._cat = category
            self._realm = realm
            self._mode = mode
            self._endpoints = {}
            self._srcSockets = []
            self._zDir = ZSocket( zmq.REQ, self._zHostDir )
            self._svc = gevent.spawn_later( 60, self._svc_refreshDir )

        def _svc_refreshDir( self ):
            self._endpoints = self._zDir.request( data = { 'realm' : self._realm, 'cat' : self._cat } )
            self._svc = gevent.spawn_later( 60, self._svc_refreshDir )

        def request( self, data ):
            z = None
            ret = None
            if 0 == len( self._srcSockets ):
                z = self._srcSockets.pop()
            elif 'random' == self._mode:
                endpoints = self._endpoints.values()
                z = ZSocket( zmq.REQ, endpoints[ random.randint( 0, len( endpoints ) - 1 ) ] )

            if z is not None:
                ret = z.request( data )
                self._srcSockets.append( z )

            return ret

    def __init__( self, host, realm, port, uid ):
        gevent.Greenlet.__init__( self )

        self._initLogging()

        self.stopEvent = gevent.event.Event()
        self._realm = realm
        self._port = port
        self.name = uid
        self._host = host

        # We keep track of all the handlers for the user per message request type
        self._handlers = {}

        # All user generated threads
        self._threads = gevent.pool.Group()

        # This socket receives all taskings for the actor and dispatch
        # the messages as requested by user
        self._opsSocket = ZMREP( 'tcp://*:%d' % self._port, isBind = True )

    def _run( self ):

        if hasattr( self, 'init' ):
            self.init()

        # We support up to n concurrent requests
        for n in range( 5 ):
            self._threads.add( gevent.spawn( self._opsHandler ) )

        self.stopEvent.wait()

        self._opsSocket.close()

        # Before we break the party, we ask gently to exit
        self.log( "Waiting for threads to finish" )
        self._threads.join( timeout = 1 )

        # Finish all the handlers, in theory we could rely on GC to eventually
        # signal the Greenlets to quit, but it's nicer to control the exact timing
        self.log( "Killing any remaining threads" )
        self._threads.kill( timeout = 10 )

        if hasattr( self, 'deinit' ):
            self.deinit()

    def _opsHandler( self ):
        z = self._opsSocket.getChild()
        while not self.stopEvent.wait( 0 ):
            msg = z.recv()
            self.log( "Received: %s" % str( msg ) )
            if msg is not None and 'req' in msg and not self.stopEvent.wait( 0 ):
                action = msg[ 'req' ]
                handler = self._handlers.get( action, self._defaultHandler )
                try:
                    ret = handler( msg )
                except gevent.GreenletExit:
                    raise
                except:
                    ret = errorMessage( 'exception', { 'st' : traceback.format_exc() } )
                if ret is True:
                    ret = successMessage()
                elif type( ret ) is str or type( ret ) is unicode:
                    ret = errorMessage( ret )
                z.send( ret )
            else:
                z.send( errorMessage( 'invalid request' ) )

    def _defaultHandler( self, msg ):
        return errorMessage( 'request type not supported by actor' )

    def stop( self ):
        self.log( "Stopping" )
        self.stopEvent.set()

    def isRunning( self ):
        return not self.ready()

    def handle( self, requestType, handlerFunction ):
        old = None
        if requestType in self._handlers:
            old = self._handlers[ requestType ]
        self._handlers[ requestType ] = handlerFunction
        return old

    def schedule( self, delay, func, *args, **kw_args ):
        if not self.stopEvent.wait( 0 ):
            self._threads.add( gevent.spawn_later( delay, self.schedule, delay, func, *args, **kw_args ) )
            try:
                func( *args, **kw_args )
            except gevent.GreenletExit:
                raise
            except:
                self.logCritical( traceback.format_exc( ) )

    def _initLogging( self ):
        syslog.openlog( '%s-%d' % ( self.__class__.__name__, os.getpid() ), facility = syslog.LOG_USER )

    def log( self, msg ):
        syslog.syslog( syslog.LOG_INFO, msg )
        msg = '%s - %s : %s' % ( int( time.time() ), self.__class__.__name__, msg )
        print( msg )

    def logCritical( self, msg ):
        syslog.syslog( syslog.LOG_ERR, msg )
        msg = '!!! %s - %s : %s' % ( int( time.time() ), self.__class__.__name__, msg )
        print( msg )

    def getActorHandle( self, category, mode = 'random' ):
        return self._ActorHandle( self._realm, category, mode )



