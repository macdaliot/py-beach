# Copyright (C) 2015  refractionPOINT
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.

import os
import gevent
import gevent.event
import gevent.pool
import traceback
import time
from beach.utils import *
from beach.utils import _TimeoutException
from beach.utils import _ZMREQ
from beach.utils import _ZMREP
from beach.utils import _ZSocket
import random
import logging
import logging.handlers
import imp
import hashlib
import inspect
import sys
from types import ModuleType

class ActorRequest( object ):
    '''Wrapper for requests to Actors. Not created directly.
    Attributes:
    ident: the identity of the requestor, if provided (as in trust)
    id: the unique identifier per request, same across retries, used for idempotent requests
    req: the action of the request
    data: data sent in the request
    '''
    def __init__( self, msg ):
        self.ident = msg[ 'mtd' ].get( 'ident', None )
        self.id = msg[ 'mtd' ][ 'id' ]
        self.req = msg[ 'mtd' ][ 'req' ]
        self.dst = msg[ 'mtd' ][ 'dst' ]
        self.data = msg[ 'data' ]

    def __str__(self):
        return 'ActorRequest( req: %s, id: %s, ident: %s, data: %s )' % ( self.req,
                                                                          self.id,
                                                                          self.ident,
                                                                          self.data )
    def __repr__(self):
        return 'ActorRequest( req: %s, id: %s, ident: %s, data: %s )' % ( self.req,
                                                                          self.id,
                                                                          self.ident,
                                                                          self.data )

class Actor( gevent.Greenlet ):

    @classmethod
    def importLib( cls, libName, className = None ):
        '''Import a user-defined lib from the proper realm.
        
        :param libName: the name of the file (module) located in the code_directry
            and realm directory
        :param className: the name of the attribute (class, func or whatever) from
            the module to be loaded

        :returns: the module or element of the module loaded
        '''
        initPath = os.path.dirname( os.path.abspath( inspect.stack()[ 1 ][ 1 ] ) )
        fileName = '%s/%s.py' % ( initPath, libName )
        if not os.path.isfile( fileName ):
            fileName = '%s/%s/__init__.py' % ( initPath, libName )
            libName = os.path.basename( initPath )
        with open( fileName, 'r' ) as hFile:
            fileHash = hashlib.sha1( hFile.read() ).hexdigest()
        libName = libName[ libName.rfind( '/' ) + 1 : ]
        modName = '%s_%s' % ( libName, fileHash )
        mod = sys.modules.get( modName, None )
        if mod is None:
            mod = imp.load_source( modName, fileName )

        if className is not None and className != '*':
            mod = getattr( mod, className )

        parentGlobals = inspect.currentframe().f_back.f_globals
        if className is not None and className == '*':
            loadModule = mod
            mod = {}
            for name, val in loadModule.__dict__.iteritems():
                if not name.startswith( '_' ) and type( name ) is not ModuleType:
                    parentGlobals[ name ] = val
                    mod[ name ] = val
        elif className is not None:
            parentGlobals[ className ] = mod
        else:
            parentGlobals[ libName ] = mod

        return mod

    '''Actors are not instantiated directly, you should create your actors as inheriting the beach.actor.Actor class.'''
    def __init__( self,
                  host,
                  realm,
                  ip,
                  port,
                  uid,
                  log_level,
                  log_dest,
                  parameters = {},
                  ident = None,
                  trusted = [],
                  n_concurrent = 1,
                  private_key = None ):
        gevent.Greenlet.__init__( self )

        self.name = uid

        self._log_level = log_level
        self._log_dest = log_dest
        self._initLogging( log_level, log_dest )

        self.stopEvent = gevent.event.Event()
        self._realm = realm
        self._ip = ip
        self._port = port
        self._host = host
        self._parameters = parameters
        self._ident = ident
        self._trusted = trusted
        self._n_concurrent = n_concurrent
        self._private_key = private_key

        self._n_free_handlers = 0

        # We keep track of all the handlers for the user per message request type
        self._handlers = {}

        # All user generated threads
        self._threads = gevent.pool.Group()

        # This socket receives all taskings for the actor and dispatch
        # the messages as requested by user
        self._opsSocket = _ZMREP( 'tcp://%s:%d' % ( self._ip, self._port ),
                                  isBind = True,
                                  private_key = self._private_key )

        self._vHandles = []

    def _run( self ):

        try:
            if hasattr( self, 'init' ):
                self.init( self._parameters )

            # Initially Actors handle one concurrent request to avoid bad surprises
            # by users not thinking about concurrency. This can be bumped up by calling
            # Actor.AddConcurrentHandler()
            for i in xrange( self._n_concurrent ):
                self.AddConcurrentHandler()

            self.stopEvent.wait()

            self._opsSocket.close()

            # Before we break the party, we ask gently to exit
            self.log( "Waiting for threads to finish" )
            self._threads.join( timeout = 30 )

            # Finish all the handlers, in theory we could rely on GC to eventually
            # signal the Greenlets to quit, but it's nicer to control the exact timing
            self.log( "Killing any remaining threads" )
            self._threads.kill( timeout = 10 )

            for v in self._vHandles:
                v.close()

            if hasattr( self, 'deinit' ):
                self.deinit()
        except:
            self.logCritical( traceback.format_exc() )

    def AddConcurrentHandler( self ):
        '''Add a new thread handling requests to the actor.'''
        self._threads.add( gevent.spawn( self._opsHandler ) )

    def _opsHandler( self ):
        z = self._opsSocket.getChild()
        self._n_free_handlers += 1
        while not self.stopEvent.wait( 0 ):
            msg = z.recv( timeout = 10 )
            if msg is False: continue
            start_time = time.time()
            try:
                request = ActorRequest( msg )
            except:
                request = None
            if not self.stopEvent.wait( 0 ):
                if request is not None:
                    self.log( "Received: %s" % request.req )

                    if request.dst != self.name:
                        ret = errorMessage( 'wrong dest' )
                        self.log( "Request is for wrong destination." )
                    elif 0 != len( self._trusted ) and request.ident not in self._trusted:
                        ret = errorMessage( 'unauthorized' )
                        self.log( "Received unauthorized request." )
                    else:
                        handler = self._handlers.get( request.req, self._defaultHandler )
                        try:
                            ret = handler( request )
                        except gevent.GreenletExit:
                            raise
                        except:
                            ret = errorMessage( 'exception', { 'st' : traceback.format_exc() } )
                            self.logCritical( ret )
                        else:
                            if ( not hasattr( ret, '__iter__' ) or
                                 0 == len( ret ) or
                                 type( ret[ 0 ] ) is not bool ):
                                ret = errorMessage( 'invalid response format', data = ret )
                            else:
                                status = ret[ 0 ]
                                if status is True:
                                    data = ret[ 1 ] if 2 == len( ret ) else {}
                                    ret = successMessage( data )
                                else:
                                    err = ret[ 1 ] if 2 <= len( ret ) else 'error'
                                    data = ret[ 2 ] if 3 == len( ret ) else {}
                                    ret = errorMessage( err, data = data )
                    z.send( ret )
                else:
                    self.logCritical( 'invalid request: %s' % str( msg ) )
                    z.send( errorMessage( 'invalid request' ) )
            self.log( "Stub call took %s seconds." % ( time.time() - start_time ) )
        self._n_free_handlers -= 1
        self.log( "Stopping processing Actor ops requests" )

    def _defaultHandler( self, msg ):
        return ( False, 'request type not supported by actor' )

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
            function receives a single parameter (the message, ActorRequest) and returns
            a tuple where the first element is True for success or False for error, the
            second element is the data (for success) or an error string (for failure)
            and the third is the data (for failure)
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
            try:
                func( *args, **kw_args )
            except gevent.GreenletExit:
                raise
            except:
                self.logCritical( traceback.format_exc() )

            if not self.stopEvent.wait( 0 ):
                self._threads.add( gevent.spawn_later( delay, self.schedule, delay, func, *args, **kw_args ) )

    def _initLogging( self, level, dest ):
        logging.basicConfig( format = "%(asctime)-15s %(message)s" )
        self._logger = logging.getLogger( self.name )
        self._logger.setLevel( level )
        self._logger.addHandler( logging.handlers.SysLogHandler( address = dest ) )

    def sleep( self, seconds ):
        gevent.sleep( seconds )

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

    def getActorHandle( self, category, mode = 'random', nRetries = None, timeout = None ):
        '''Get a virtual handle to actors in the cluster.

        :param category: the name of the category holding actors to get the handle to
        :param mode: the method actors are queried by the handle, currently
            handles: random
        :param nRetries: number of times the handle should attempt to retry the request if
            it times out
        :param timeout: number of seconds to wait before re-issuing a request or failing
        :returns: an ActorHandle
        '''
        v = ActorHandle( self._realm,
                         category,
                         mode,
                         ident = self._ident,
                         nRetries = nRetries,
                         timeout = timeout )
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

    def getActorHandleGroup( self, categoryRoot, mode = 'random', nRetries = None, timeout = None ):
        '''Get a virtual handle to actors in the cluster.

        :param category: the name of the category holding actors to get the handle to
        :param mode: the method actors are queried by the handle, currently
            handles: random
        :param nRetries: number of times the handle should attempt to retry the request if
            it times out
        :param timeout: number of seconds to wait before re-issuing a request or failing
        :returns: an ActorHandle
        '''
        v = ActorHandleGroup( self._realm,
                              categoryRoot,
                              mode,
                              ident = self._ident,
                              nRetries = nRetries,
                              timeout = timeout )
        self._vHandles.append( v )
        return v

    def yieldCpu( self ):
        '''Yield the CPU to another coroutine, if needed during long tasks.
        '''
        return gevent.sleep( 0 )



class ActorResponse( object ):
    '''Wrapper for responses to requests to Actors. Not created directly.
    Attributes:
    isTimedOut: boolean indicating if the request timed out
    isSuccess: boolean indicating if the request was successful
    error: string with error message if present
    data: data sent back in the response
    '''
    def __init__( self, msg ):
        if msg is False:
            self.isTimedOut = True
            self.isSuccess = False
            self.error = 'timeout'
            self.data = {}
        else:
            self.isTimedOut = False
            self.isSuccess = msg[ 'status' ][ 'success' ]
            self.error = msg[ 'status' ].get( 'error', '' )
            self.data = msg.get( 'data', {} )

    def __str__( self ):
        return 'ActorResponse( isSuccess: %s, isTimedOut: %s, error: %s, data: %s )' % ( self.isSuccess,
                                                                                         self.isTimedOut,
                                                                                         self.error,
                                                                                         self.data )

    def __repr__(self):
        return 'ActorResponse( isSuccess: %s, isTimedOut: %s, error: %s, data: %s )' % ( self.isSuccess,
                                                                                         self.isTimedOut,
                                                                                         self.error,
                                                                                         self.data )



# ActorHandle is not meant to be created manually.
# They are returned by either the Beach.getActorHandle()
# or Actor.getActorHandle().
class ActorHandle ( object ):
    _zHostDir = None
    _zDir = []
    _private_key = None

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
            if isMessageSuccess( msg ) and 'endpoints' in msg[ 'data' ]:
                msg = msg[ 'data' ][ 'endpoints' ]
            else:
                msg = False
        return msg

    @classmethod
    def _setHostDirInfo( cls, zHostDir, private_key = None ):
        cls._private_key = private_key
        if type( zHostDir ) is not tuple and type( zHostDir ) is not list:
            zHostDir = ( zHostDir, )
        if cls._zHostDir is None:
            cls._zHostDir = zHostDir
            for h in zHostDir:
                cls._zDir.append( _ZMREQ( h, isBind = False, private_key = cls._private_key ) )

    def __init__( self,
                  realm,
                  category,
                  mode = 'random',
                  nRetries = None,
                  timeout = None,
                  ident = None ):
        self._cat = category
        self._nRetries = nRetries
        self._timeout = timeout
        self._realm = realm
        self._mode = mode
        self._ident = ident
        self._endpoints = {}
        self._srcSockets = []
        self._threads = gevent.pool.Group()
        self._threads.add( gevent.spawn_later( 0, self._svc_refreshDir ) )
        self._quick_refresh_timeout = 15
        self._initialRefreshDone = gevent.event.Event()
        self._affinityCache = {}
        self._affinityOrder = None

    def _updateDirectory( self ):
        newDir = self._getDirectory( self._realm, self._cat )
        if newDir is not False:
            self._endpoints = newDir
            if not self._initialRefreshDone.isSet():
                self._initialRefreshDone.set()

    def _svc_refreshDir( self ):
        self._updateDirectory()
        if ( 0 == len( self._endpoints ) ) and ( 0 < self._quick_refresh_timeout ):
            self._quick_refresh_timeout -= 1
            # No Actors yet, be more agressive to look for some
            self._threads.add( gevent.spawn_later( 2, self._svc_refreshDir ) )
        else:
            self._threads.add( gevent.spawn_later( 60 + random.randint( 0, 10 ), self._svc_refreshDir ) )

    def request( self, requestType, data = {}, timeout = None, key = None, nRetries = None ):
        '''Issue a request to the actor category of this handle.

        :param requestType: the type of request to issue
        :param data: a dict of the data associated with the request
        :param timeout: the number of seconds to wait for a response
        :param key: when used in 'affinity' mode, the key is the main parameter
            to evaluate to determine which Actor to send the request to, in effect
            it is the key to the hash map of Actors
        :param nRetries: the number of times the request will be re-sent if it
            times out, meaning a timeout of 5 and a retry of 3 could result in
            a request taking 15 seconds to return
        :returns: the response to the request as an ActorResponse
        '''
        z = None
        z_ident = None
        ret = False
        curRetry = 0
        affinityKey = None

        if nRetries is None:
            nRetries = self._nRetries
            if nRetries is None:
                nRetries = 0

        if timeout is None:
            timeout = self._timeout
        if 0 == timeout:
            timeout = None

        while curRetry <= nRetries:
            try:
                # We use the timeout to wait for an available node if none
                # exists
                with gevent.Timeout( timeout, _TimeoutException ):

                    self._initialRefreshDone.wait( timeout = timeout if timeout is not None else self._timeout )

                    while z is None:
                        if 'affinity' == self._mode and key is not None:
                            # Affinity is currently a soft affinity, meaning the set of Actors
                            # is not locked, if it changes, affinity is re-computed without migrating
                            # any previous affinities. Therefore, I suggest a good cooldown before
                            # starting to process with affinity after the Actors have been spawned.
                            orderedEndpoints  = sorted( self._endpoints.items(),
                                                        key = lambda x: x.__getitem__( 0 ) )
                            orderHash = tuple( [ x[ 0 ] for x in orderedEndpoints ] ).__hash__()
                            if self._affinityOrder is None or self._affinityOrder != orderHash:
                                self._affinityOrder = orderHash
                                self._affinityCache = {}
                            affinityKey = ( hash( key ) % len( orderedEndpoints ) )
                            if affinityKey in self._affinityCache:
                                z, z_ident = self._affinityCache[ affinityKey ]
                            else:
                                z_ident, z = orderedEndpoints[ affinityKey ]
                                z = _ZSocket( zmq.REQ, z, private_key = self._private_key )
                                if z is not None:
                                    self._affinityCache[ affinityKey ] = z, z_ident
                        elif 0 != len( self._srcSockets ):
                            # Prioritize existing connections, only create new one
                            # based on the mode when we have no connections available
                            z, z_ident = self._srcSockets.pop()
                        elif 'random' == self._mode:
                            endpoints = self._endpoints.keys()
                            if 0 != len( endpoints ):
                                z_ident = endpoints[ random.randint( 0, len( endpoints ) - 1 ) ]
                                z = _ZSocket( zmq.REQ,
                                              self._endpoints[ z_ident ],
                                              private_key = self._private_key )
                        if z is None:
                            gevent.sleep( 0.001 )
            except _TimeoutException:
                curRetry += 1

            if z is not None and curRetry <= nRetries:
                envelope = { 'data' : data,
                             'mtd' : { 'ident' : self._ident,
                                       'req' : requestType,
                                       'id' : str( uuid.uuid4() ),
                                       'dst' : z_ident } }

                ret = z.request( envelope, timeout = timeout )

                ret = ActorResponse( ret )
                # If we hit a timeout or wrong dest we don't take chances
                # and remove that socket
                if not ret.isTimedOut and ( ret.isSuccess or ret.error != 'wrong dest' ):
                    if 'affinity' != self._mode:
                        self._srcSockets.append( ( z, z_ident ) )
                    break
                else:
                    if 'affinity' == self._mode:
                        del( self._affinityCache[ affinityKey ] )
                    z.close()
                    z = None
                    curRetry += 1

        if z is not None:
            self._srcSockets.append( ( z, z_ident ) )

        if ret is None or ret is False:
            ret = ActorResponse( ret )

        return ret

    def broadcast( self, requestType, data = {} ):
        '''Issue a request to the all actors in the category of this handle.

        :param requestType: the type of request to issue
        :param data: a dict of the data associated with the request
        :returns: True since no validation on the reception or reply from any
            specific endpoint is made
        '''
        ret = True

        envelope = { 'data' : data,
                     'mtd' : { 'ident' : self._ident,
                               'req' : requestType,
                               'id' : str( uuid.uuid4() ) } }

        for z_ident, endpoint in self._endpoints.items():
            z = _ZSocket( zmq.REQ, endpoint, private_key = self._private_key )
            if z is not None:
                envelope[ 'mtd' ][ 'dst' ] = z_ident
                gevent.spawn( z.request, envelope )

        gevent.sleep( 0 )

        return ret

    def shoot( self, requestType, data = {}, timeout = None, key = None, nRetries = None ):
        '''Send a message to the one actor without waiting for a response.

        :param requestType: the type of request to issue
        :param data: a dict of the data associated with the request
        :param timeout: the number of seconds to wait for a response
        :param key: when used in 'affinity' mode, the key is the main parameter
            to evaluate to determine which Actor to send the request to, in effect
            it is the key to the hash map of Actors
        :param nRetries: the number of times the request will be re-sent if it
            times out, meaning a timeout of 5 and a retry of 3 could result in
            a request taking 15 seconds to return
        :returns: True since no validation on the reception or reply
            the endpoint is made
        '''
        ret = True

        gevent.spawn( self.request, requestType, data, timeout = timeout, key = key, nRetries = nRetries )
        gevent.sleep( 0 )

        return ret

    def isAvailable( self ):
        '''Checks to see if any actors are available to respond to a query of this handle.

        :returns: True if at least one actor is available
        '''
        self._initialRefreshDone.wait( 2 )
        return ( 0 != len( self._endpoints ) )

    def getNumAvailable( self ):
        '''Checks to see the number of actors available to respond to a query of this handle.

        :returns: number of available actors
        '''
        self._initialRefreshDone.wait( 2 )
        return len( self._endpoints )

    def close( self ):
        '''Close all threads and resources associated with this handle.
        '''
        self._threads.kill()

    def forceRefresh( self ):
        '''Force a refresh of the handle metadata with nodes in the cluster. This is optional
           since a periodic refresh is already at an interval frequent enough for most use.
        '''
        self._updateDirectory()

class ActorHandleGroup( object ):
    _zHostDir = None
    _zDir = []
    _private_key = None

    def __init__( self,
                  realm,
                  categoryRoot,
                  mode = 'random',
                  nRetries = None,
                  timeout = None,
                  ident = None ):
        self._realm = realm
        self._categoryRoot = categoryRoot
        if not self._categoryRoot.endswith( '/' ):
            self._categoryRoot += '/'
        self._mode = mode
        self._nRetries = nRetries
        self._timeout = timeout
        self._ident = ident
        self._quick_refresh_timeout = 15
        self._initialRefreshDone = gevent.event.Event()
        self._threads = gevent.pool.Group()
        self._threads.add( gevent.spawn_later( 0, self._svc_periodicRefreshCats ) )
        self._handles = {}

    @classmethod
    def _setHostDirInfo( cls, zHostDir, private_key = None ):
        cls._private_key = private_key
        if type( zHostDir ) is not tuple and type( zHostDir ) is not list:
            zHostDir = ( zHostDir, )
        if cls._zHostDir is None:
            cls._zHostDir = zHostDir
            for h in zHostDir:
                cls._zDir.append( _ZMREQ( h, isBind = False, private_key = cls._private_key ) )

    @classmethod
    def _getCategories( cls, realm, catRoot ):
        msg = False
        if 0 != len( cls._zDir ):
            z = cls._zDir[ random.randint( 0, len( cls._zDir ) - 1 ) ]
            # These requests can be sent to the directory service of a HostManager
            # or the ops service of the HostManager. Directory service is OOB from the
            # ops but is only available locally to Actors. The ops is available from outside
            # the host. So if the ActorHandle is created by an Actor, it goes to the dir_svc
            # and if it's created from outside components through a Beach it goes to
            # the ops.
            cats = None
            resp = z.request( { 'req' : 'get_cats_under', 'cat' : catRoot, 'realm' : realm }, timeout = 10 )
            if isMessageSuccess( resp ):
                cats = resp[ 'data' ][ 'categories' ]
        return cats

    def _refreshCats( self ):
        cats = self._getCategories( self._realm, self._categoryRoot )
        if cats is not False:
            categories = []
            for cat in cats:
                cat = cat.replace( self._categoryRoot, '' ).split( '/' )
                cat = cat[ 0 ] if ( 0 != len( cat ) or 1 == len( cat ) ) else cat[ 1 ]
                categories.append( '%s%s/' % ( self._categoryRoot, cat ) )

            for cat in categories:
                if cat not in self._handles:
                    self._handles[ cat ] = ActorHandle( self._realm,
                                                        cat,
                                                        mode = self._mode,
                                                        nRetries = self._nRetries,
                                                        timeout = self._timeout,
                                                        ident = self._ident )

            for cat in self._handles.keys():
                if cat not in categories:
                    self._handles[ cat ].close()
                    del( self._handles[ cat ] )
            if not self._initialRefreshDone.isSet():
                self._initialRefreshDone.set()
        return cats

    def _svc_periodicRefreshCats( self ):
        cats = self._refreshCats()
        if cats is False or 0 == len( cats ) and 0 < self._quick_refresh_timeout:
            self._quick_refresh_timeout -= 1
            # No Actors yet, be more agressive to look for some
            self._threads.add( gevent.spawn_later( 10, self._svc_periodicRefreshCats ) )
        else:
            self._threads.add( gevent.spawn_later( ( 60 * 5 ) + random.randint( 0, 10 ), self._svc_periodicRefreshCats ) )

    def shoot( self, requestType, data = {}, timeout = None, key = None, nRetries = None ):
        '''Send a message to the one actor in each sub-category without waiting for a response.

        :param requestType: the type of request to issue
        :param data: a dict of the data associated with the request
        :param timeout: the number of seconds to wait for a response
        :param nRetries: the number of times the request will be re-sent if it
            times out, meaning a timeout of 5 and a retry of 3 could result in
            a request taking 15 seconds to return
        :returns: True since no validation on the reception or reply
            the endpoint is made
        '''
        self._initialRefreshDone.wait( timeout = timeout if timeout is not None else self._timeout )

        for h in self._handles.values():
            h.shoot( requestType, data, timeout = timeout, key = key, nRetries = nRetries )

    def getNumAvailable( self ):
        '''Checks to see the number of categories served by this HandleGroup.

        :returns: number of available categories
        '''
        self._initialRefreshDone.wait( 2 )
        return len( self._handles )

    def close( self ):
        '''Close all threads and resources associated with this handle group.
        '''
        for h in self._handles.values():
            h.close()

        self._threads.kill()

    def forceRefresh( self ):
        '''Force a refresh of the handle metadata with nodes in the cluster. This is optional
           since a periodic refresh is already at an interval frequent enough for most use.
        '''
        self._refreshCats()
        for handle in self._handles.values():
            handle._updateDirectory()