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
            self._zDir = ZMREQ( self._zHostDir, isBind = False )
            self._threads = gevent.pool.Group()
            self._threads.add( gevent.spawn_later( 0, self._svc_refreshDir ) )

        def _svc_refreshDir( self ):
            msg = self._zDir.request( data = { 'realm' : self._realm, 'cat' : self._cat } )
            if isMessageSuccess( msg ) and 'endpoints' in msg:
                self._endpoints = msg[ 'endpoints' ]
            print( "VIRTUAL HANDLE rEQ" )
            if 0 == len( self._endpoints ):
                # No Actors yet, be more agressive to look for some
                self._threads.add( gevent.spawn_later( 2, self._svc_refreshDir ) )
            else:
                self._threads.add( gevent.spawn_later( 60, self._svc_refreshDir ) )

        def request( self, requestType, data = {}, timeout = None ):
            z = None
            ret = False

            try:
                # We use the timeout to wait for an available node if none
                # exists
                with gevent.Timeout( timeout, TimeoutException ):
                    while z is None:
                        if 0 != len( self._srcSockets ):
                            # Prioritize existing connections, only create new one
                            # based on the mode when we have no connections available
                            z = self._srcSockets.pop()
                        elif 'random' == self._mode:
                            endpoints = self._endpoints.values()
                            if 0 != len( endpoints ):
                                z = ZSocket( zmq.REQ, endpoints[ random.randint( 0, len( endpoints ) - 1 ) ] )
                        if z is None:
                            gevent.sleep( 0.001 )
            except TimeoutException:
                print( "TIMEOUT GETTING PONGERS" )
                pass

            if z is not None:

                if type( data ) is not dict:
                    data = { 'data' : data }
                data[ 'req' ] = requestType

                ret = z.request( data, timeout = timeout )
                # If we hit a timeout we don't take chances
                # and remove that socket
                if ret is not False:
                    self._srcSockets.append( z )
                else:
                    z.close()

            return ret

        def close( self ):
            self._threads.kill()

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

        self._vHandles = []

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

        for v in self._vHandles:
            v.close()

        if hasattr( self, 'deinit' ):
            self.deinit()

    def _opsHandler( self ):
        z = self._opsSocket.getChild()
        while not self.stopEvent.wait( 0 ):
            msg = z.recv()
            if msg is not None and 'req' in msg and not self.stopEvent.wait( 0 ):
                action = msg[ 'req' ]
                self.log( "Received: %s" % action )
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
        #syslog.openlog( '%s-%d' % ( self.__class__.__name__, os.getpid() ), facility = syslog.LOG_USER )
        pass

    def log( self, msg ):
        syslog.syslog( syslog.LOG_INFO, msg )
        msg = '%s - %s : %s' % ( int( time.time() ), self.__class__.__name__, msg )
        print( msg )

    def logCritical( self, msg ):
        syslog.syslog( syslog.LOG_ERR, msg )
        msg = '!!! %s - %s : %s' % ( int( time.time() ), self.__class__.__name__, msg )
        print( msg )

    def getActorHandle( self, category, mode = 'random' ):
        v = self._ActorHandle( self._realm, category, mode )
        self._vHandles.append( v )
        return v



