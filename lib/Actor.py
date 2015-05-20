import gevent
from gevent import Greenlet
from gevent.event import Event
import zmq.green as zmq
import json
import traceback
import time
from Utils import *

class Actor( Greenlet ):

    # Available to the Actor implementation:
    #  stopEvent, an Event indicating the Actor should stop cleanly
    #  config, json configuration
    #  init, function to define to initialize the Actor
    #  deinit, function to define to cleanup the Actor
    #  handle, set the function to call on messages received on a socket
    #  schedule, set a recurring function

    def __init__( self, realm, port, uid ):
        Greenlet.__init__( self )

        self.stopEvent = Event()
        self.realm = realm
        self.port = port
        self.name = uid

        # We keep track of all the handlers for the user per message request type
        self.handlers = {}

        self.reqHandlers = []

    def _run( self ):

        if hasattr( self, 'init' ):
            self.init()

        # This socket receives all taskings for the actor and dispatch
        # the messages as requested by user
        self._multiplexRep( 'tcp://*:%d' % self.port, 'inproc://%s' % ( self.name, ) )

        # We support up to n concurrent requests
        for n in range( 5 ):
            self.reqHandlers.append( gevent.spawn( self._opsHandler ) )

        self.stopEvent.wait()

        # Finish all the handlers
        for h in self.reqHandlers:
            h.join( timeout = 2 )
            if not h.ready():
                self.logCritical( "Waited for 2 seconds for handler/scheduled %s to finish, timeout, killing." % str( h ) )
                h.kill()

        if hasattr( self, 'deinit' ):
            self.deinit()

    def _multiplexRep(self, frontEnd, backEnd ):
        zCtx = zmq.Context()
        zFront = zCtx.socket( zmq.ROUTER )
        zBack = zCtx.socket( zmq.DEALER )
        zFront.bind( frontEnd )
        zBack.bind( backEnd )

        gevent.spawn( self._proxy, zFront, zBack )
        gevent.spawn( self._proxy, zBack, zFront )

    def _proxy(self, zFrom, zTo ):
        while not self.stopEvent.wait( 0 ):
            msg = zFrom.recv_multipart()
            zTo.send_multipart( msg )

    def _opsHandler( self ):
        z = ZSocket( zmq.REP, 'inproc://%s' % ( self.name, ) )
        while not self.stopEvent.wait( 0 ):
            msg = z.recv()
            if msg is not None and 'req' in msg and not self.stopEvent.wait( 0 ):
                action = msg[ 'req' ]
                handler = self.handlers.get( action, self._defaultHandler )
                try:
                    ret = handler( msg )
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
        if requestType in self.handlers:
            old = self.handlers[ requestType ]
        self.handlers[ requestType ] = handlerFunction
        return old

    def schedule( self, delay, func, *args, **kw_args ):
        if not self.stopEvent.wait( 0 ):
            gevent.spawn_later( delay, self.schedule, delay, func, *args, **kw_args )
            try:
                self.reqHandlers.append( gevent.getcurrent() )
                func( *args, **kw_args )
            except:
                self.logCritical( traceback.format_exc( ) )
            finally:
                self.reqHandlers.remove( gevent.getcurrent() )

    def log( self, msg ):
        msg = '%s - %s : %s' % ( int( time.time() ), self.__class__.__name__, msg )
        print( msg )

    def logCritical( self, msg ):
        msg = '!!! %s - %s : %s' % ( int( time.time() ), self.__class__.__name__, msg )
        print( msg )