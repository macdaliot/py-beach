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

        # This socket receives all taskings for the actor and dispatch
        # the messages as requested by user
        self._multiplexRep( 'tcp://*:%d' % self.port, 'inproc://%s' % ( self.name, ) )

        # We support up to n concurrent requests
        for n in range( 5 ):
            gevent.spawn( self._opsHandler )

        # We keep track of all the handlers for the user per message request type
        self.handlers = {}

        # We use a simple counter to determine if all handlers
        # for this specific actor have finished executing to
        # then call the deinit function safely
        self.nRunning = 0

    def _run( self ):

        if hasattr( self, 'init' ):
            self.init()

        self.stopEvent.wait()

        while 0 != self.nRunning:
            self.log( "Waiting on %d handlers still running..." % self.nRunning )
            gevent.sleep( 1 )

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
            gevent.spawn_later( 0, func, *args, **kw_args )
            gevent.spawn_later( delay, self.schedule, delay, func, *args, **kw_args )

    def log( self, msg ):
        msg = '%s - %s : %s' % ( int( time.time() ), self.__class__.__name__, msg )
        print( msg )

    def logCritical( self, msg ):
        msg = '!!! %s - %s : %s' % ( int( time.time() ), self.__class__.__name__, msg )
        print( msg )