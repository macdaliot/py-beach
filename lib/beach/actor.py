import os
import gevent
import gevent.event
import gevent.pool
import zmq.green as zmq
import traceback
import time
from beach.utils import *
from beach.utils import _TimeoutException
from beach.utils import _ZMREQ
from beach.utils import _ZMREP
from beach.utils import _ZSocket
import random
import logging

class Actor( gevent.Greenlet ):
    '''Actors are not instantiated directly, you should create your actors as inheriting the beach.actor.Actor class.'''
    def __init__( self, host, realm, ip, port, uid ):
        gevent.Greenlet.__init__( self )

        self._initLogging()

        self.stopEvent = gevent.event.Event()
        self._realm = realm
        self._ip = ip
        self._port = port
        self.name = uid
        self._host = host

        # We keep track of all the handlers for the user per message request type
        self._handlers = {}

        # All user generated threads
        self._threads = gevent.pool.Group()

        # This socket receives all taskings for the actor and dispatch
        # the messages as requested by user
        self._opsSocket = _ZMREP( 'tcp://%s:%d' % ( self._ip, self._port ), isBind = True )

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
        self.log( "Stopping processing Actor ops requests" )

    def _defaultHandler( self, msg ):
        return errorMessage( 'request type not supported by actor' )

    def stop( self ):
        '''Stop the actor and its threads.'''
        self.log( "Stopping" )
        self.stopEvent.set()

    def isRunning( self ):
        '''Checks if the actor is currently running.

        :returns: True if the actor is running
        '''
        return not self.ready()

    def handle( self, requestType, handlerFunction ):
        '''Initiates a callback for a specific type of request.

        :param requestType: the string representing the type of request to handle
        :param handlerFunction: the function that will handle those requests, this
            function receives a single parameter (the message) and returns a message
            to reply to the message. If it returns True, a generic success message will
            be replied, and if it returns a simple string, it will reply a generic error
            message where the string is the error message. To return data, return a dict.
        :returns: the previous handler for the request type or None if None existed
        '''
        old = None
        if requestType in self._handlers:
            old = self._handlers[ requestType ]
        self._handlers[ requestType ] = handlerFunction
        return old

    def schedule( self, delay, func, *args, **kw_args ):
        '''Schedule a recurring function.

        :param delay: the number of seconds interval between calls
        :param func: the function to call at interval
        :param args: positional arguments to the function
        :param kw_args: keyword arguments to the function
        '''
        if not self.stopEvent.wait( 0 ):
            self._threads.add( gevent.spawn_later( delay, self.schedule, delay, func, *args, **kw_args ) )
            try:
                func( *args, **kw_args )
            except gevent.GreenletExit:
                raise
            except:
                self.logCritical( traceback.format_exc( ) )

    def _initLogging( self ):
        logging.basicConfig( format = "%(asctime)-15s %(message)s" )
        self._logger = logging.getLogger()
        self._logger.setLevel( logging.INFO )

    def log( self, msg ):
        '''Log debug statements.

        :param msg: the message to log
        '''
        self._logger.info( '%s : %s', self.__class__.__name__, msg )

    def logCritical( self, msg ):
        '''Log errors.

        :param msg: the message to log
        '''
        self._logger.error( '%s : %s', self.__class__.__name__, msg )

    def getActorHandle( self, category, mode = 'random' ):
        '''Get a virtual handle to actors in the cluster.

        :param category: the name of the category holding actors to get the handle to
        :param mode: the method actors are queried by the handle, currently
            handles: random
        :returns: an ActorHandle
        '''
        v = ActorHandle( self._realm, category, mode )
        self._vHandles.append( v )
        return v

    def isCategoryAvailable( self, category ):
        '''Checks if actors are available in the category.

        :param category: the category to look for actors in
        :returns: True if actors are available
        '''
        isAvailable = False

        if 0 != ActorHandle._getNAvailableInCat( self._realm, category ):
            isAvailable = True

        return isAvailable


# ActorHandle is not meant to be created manually.
# They are returned by either the Beach.getActorHandle()
# or Actor.getActorHandle().
class ActorHandle ( object ):
        _zHostDir = None
        _zDir = []

        @classmethod
        def _getNAvailableInCat( cls, realm, cat ):
            nAvailable = 0
            newDir = cls._getDirectory( realm, cat )
            if newDir is not False:
                nAvailable = len( newDir )
            return nAvailable

        @classmethod
        def _getDirectory( cls, realm, cat ):
            msg = False
            if 0 != len( cls._zDir ):
                z = cls._zDir[ random.randint( 0, len( cls._zDir ) - 1 ) ]
                # These requests can be sent to the directory service of a HostManager
                # or the ops service of the HostManager. Directory service is OOB from the
                # ops but is only available locally to Actors. The ops is available from outside
                # the host. So if the ActorHandle is created by an Actor, it goes to the dir_svc
                # and if it's created from outside components through a Beach it goes to
                # the ops.
                msg = z.request( data = { 'req' : 'get_dir', 'realm' : realm, 'cat' : cat } )
                if isMessageSuccess( msg ) and 'endpoints' in msg:
                    msg = msg[ 'endpoints' ]
                else:
                    msg = False
            return msg

        @classmethod
        def _setHostDirInfo( cls, zHostDir ):
            if type( zHostDir ) is not tuple and type( zHostDir ) is not list:
                zHostDir = ( zHostDir, )
            if cls._zHostDir is None:
                cls._zHostDir = zHostDir
                for h in zHostDir:
                    cls._zDir.append( _ZMREQ( h, isBind = False ) )

        def __init__( self, realm, category, mode = 'random' ):
            self._cat = category
            self._realm = realm
            self._mode = mode
            self._endpoints = {}
            self._srcSockets = []
            self._threads = gevent.pool.Group()
            self._threads.add( gevent.spawn_later( 0, self._svc_refreshDir ) )

        def _svc_refreshDir( self ):
            newDir = self._getDirectory( self._realm, self._cat )
            if newDir is not False:
                self._endpoints = newDir
            if 0 == len( self._endpoints ):
                # No Actors yet, be more agressive to look for some
                self._threads.add( gevent.spawn_later( 2, self._svc_refreshDir ) )
            else:
                self._threads.add( gevent.spawn_later( 60, self._svc_refreshDir ) )

        def request( self, requestType, data = {}, timeout = None ):
            '''Issue a request to the actor category of this handle.

            :param requestType: the type of request to issue
            :param data: a dict of the data associated with the request
            :param timeout: the number of seconds to wait for a response
            :returns: the response to the request as a dict, or False in the event
                the request failed or timed out
            '''
            z = None
            ret = False

            try:
                # We use the timeout to wait for an available node if none
                # exists
                with gevent.Timeout( timeout, _TimeoutException ):
                    while z is None:
                        if 0 != len( self._srcSockets ):
                            # Prioritize existing connections, only create new one
                            # based on the mode when we have no connections available
                            z = self._srcSockets.pop()
                        elif 'random' == self._mode:
                            endpoints = self._endpoints.values()
                            if 0 != len( endpoints ):
                                z = _ZSocket( zmq.REQ, endpoints[ random.randint( 0, len( endpoints ) - 1 ) ] )
                        if z is None:
                            gevent.sleep( 0.001 )
            except _TimeoutException:
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

        def isAvailable( self ):
            '''Checks to see if any actors are available to respond to a query of this handle.

            :returns: True if at least one actor is available
            '''
            return ( 0 != len( self._endpoints ) )

        def close( self ):
            '''Close all threads and resources associated with this handle.
            '''
            self._threads.kill()