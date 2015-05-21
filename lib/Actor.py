import gevent
import zmq.green as zmq
import json
import traceback
import time
from Utils import *
import random

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

        self.stopEvent = gevent.Event()
        self._realm = realm
        self._port = port
        self.name = uid
        self._host = host

        # We keep track of all the handlers for the user per message request type
        self._handlers = {}

        self._threads = gevent.pool.Group()

    def _run( self ):

        if hasattr( self, 'init' ):
            self.init()

        # This socket receives all taskings for the actor and dispatch
        # the messages as requested by user
        self._multiplexRep( 'tcp://*:%d' % self._port, 'inproc://%s' % ( self.name, ) )

        # We support up to n concurrent requests
        for n in range( 5 ):
            self._threads.add( gevent.spawn( self._opsHandler ) )

        self.stopEvent.wait()

        # Before we break the party, we ask gently to exit
        gevent.sleep( 10 )

        # Finish all the handlers, in theory we could rely on GC to eventually
        # signal the Greenlets to quit, but it's nicer to control the exact timing
        self._threads.kill( timeout = 10 )

        if hasattr( self, 'deinit' ):
            self.deinit()

    def _multiplexRep( self, frontEnd, backEnd ):
        zCtx = zmq.Context()
        zFront = zCtx.socket( zmq.ROUTER )
        zBack = zCtx.socket( zmq.DEALER )
        zFront.bind( frontEnd )
        zBack.bind( backEnd )

        self._threads.add( gevent.spawn( self._proxy, zFront, zBack ) )
        self._threads.add( gevent.spawn( self._proxy, zBack, zFront ) )

    def _proxy( self, zFrom, zTo ):
        while not self.stopEvent.wait( 0 ):
            msg = zFrom.recv_multipart()
            if not self.stopEvent.wait( 0 ):
                zTo.send_multipart( msg )

    def _opsHandler( self ):
        z = ZSocket( zmq.REP, 'inproc://%s' % ( self.name, ) )
        while not self.stopEvent.wait( 0 ):
            msg = z.recv()
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

    def log( self, msg ):
        msg = '%s - %s : %s' % ( int( time.time() ), self.__class__.__name__, msg )
        print( msg )

    def logCritical( self, msg ):
        msg = '!!! %s - %s : %s' % ( int( time.time() ), self.__class__.__name__, msg )
        print( msg )

    def getActorHandle( self, category, mode = 'random' ):
        return self._ActorHandle( self._realm, category, mode )



