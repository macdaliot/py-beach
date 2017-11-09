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
import os.path
import gevent
import gevent.event
import gevent.pool
import traceback
import time
import uuid
import beach.utils
from beach.utils import _TimeoutException
from beach.utils import _ZMREQ
from beach.utils import _ZMREP
from beach.utils import loadModuleFrom
from beach.utils import isMessageSuccess
from beach.utils import errorMessage
from beach.utils import successMessage
import random
import logging
import logging.handlers
import imp
import hashlib
import inspect
import sys
import urllib2
import copy
import Queue
from types import ModuleType
import syslog
from sets import Set

def withLogException( f, actor = None ):
    def _tmp( *args, **kw_args ):
        try:
            return f( *args, **kw_args )
        except gevent.GreenletExit:
            raise
        except:
            if actor is not None:
                actor.logCritical( traceback.format_exc() )
            else:
                syslog.syslog( traceback.format_exc() )
    return _tmp

def err_print( msg ):
    sys.stderr.write( '%s\n' % msg )

class ActorRequest( object ):
    '''Wrapper for requests to Actors. Not created directly.
    Attributes:
    ident: the identity of the requestor, if provided (as in trust)
    id: the unique identifier per request, same across retries, used for idempotent requests
    req: the action of the request
    data: data sent in the request
    '''
    __slots__ = [ 'ident', 'id', 'req', 'dst', 'data' ]

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

    _code_directory_root = None

    @classmethod
    def importLib( cls, libName, className = None ):
        '''Import a user-defined lib from the proper realm.
        
        :param libName: the name of the file (module) located in the code_directry
            and realm directory
        :param className: the name of the attribute (class, func or whatever) from
            the module to be loaded

        :returns: the module or element of the module loaded
        '''
        if '://' in libName:
            # This is an absolute path
            fileName = libName
        else:
            parentGlobals = inspect.currentframe().f_back.f_globals
            realm = parentGlobals.get( '_beach_realm', '' )
            if libName.startswith( '.' ) or cls._code_directory_root is None:
                # For relative paths we look at the parent's path
                if '_beach_path' not in parentGlobals:
                    if '/' in parentGlobals[ '__file__' ]:
                        initPath = 'file://%s' % parentGlobals[ '__file__' ][ : parentGlobals[ '__file__' ].rfind( '/' ) ]
                    else:
                        initPath = 'file://.'
                else:
                    initPath = parentGlobals[ '_beach_path' ][ : parentGlobals[ '_beach_path' ].rfind( '/' ) ]
            else:
                # This is a realm-relative path
                initPath = '%s/%s/' % ( cls._code_directory_root, realm )
                
            fileName = '%s/%s.py' % ( initPath, libName )

        try:
            mod = loadModuleFrom( fileName, realm )
        except urllib2.URLError:
            if os.path.isfile( fileName ):
                raise
            fileName = '%s/%s/__init__.py' % ( initPath, libName )
            mod = loadModuleFrom( fileName, realm )

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

    @classmethod
    def readRelativeFile( cls, relFilePath ):
        '''Read the contents of a file relative to the caller in Beach.
        
        :param relFilePath: the path to the file to read

        :returns: the content of the file
        '''
        parentGlobals = inspect.currentframe().f_back.f_globals
        realm = parentGlobals.get( '_beach_realm', '' )
        if relFilePath.startswith( '.' ) or cls._code_directory_root is None:
            # For relative paths we look at the parent's path
            if '_beach_path' not in parentGlobals:
                initPath = 'file://%s' % parentGlobals[ '__file__' ][ : parentGlobals[ '__file__' ].rfind( '/' ) ]
            else:
                initPath = parentGlobals[ '_beach_path' ][ : parentGlobals[ '_beach_path' ].rfind( '/' ) ]
        else:
            # This is a realm-relative path
            initPath = '%s/%s' % ( cls._code_directory_root, realm )

        fileName = '%s/%s' % ( initPath, relFilePath )
        if fileName.startswith( 'file://' ):
            fileName = 'file://%s' % os.path.abspath( fileName[ 7 : ] )
            
        hUrl = urllib2.urlopen( fileName )
        ret = hUrl.read()
        hUrl.close()
        return ret

    @classmethod
    def initTestActor( cls, parameters = {}, resources = {}, mock_actor_handle = None ):
        uid = str( uuid.uuid4() )
        a = cls( '127.0.0.1',
                 'global',
                 '127.0.0.1',
                 None,
                 uid,
                 10,
                 '/dev/log',
                 '/tmp/%s_beach.conf' % uid,
                 parameters = parameters,
                 resources = resources,
                 ident = None,
                 trusted = [],
                 n_concurrent = 1,
                 private_key = None,
                 is_drainable = False,
                 is_also_log_to_stderr = True,
                 mock_actor_handle = mock_actor_handle )
        # If the actor is tested via the Python interactive command prompt we need to specifically
        # tell it to yield to the initialization function initially.
        a.start()
        a.sleep( 1 )
        return a

    @classmethod
    def mockRequest( cls, data = {} ):
        return ActorRequest( { 'mtd' : { 'id' : uuid.uuid4(), 'req' : 'test', 'dst' : 'test' }, 'data' : data } )

    '''Actors are not instantiated directly, you should create your actors as inheriting the beach.actor.Actor class.'''
    def __init__( self,
                  host,
                  realm,
                  ip,
                  port,
                  uid,
                  log_level,
                  log_dest,
                  beach_config_path,
                  parameters = {},
                  resources = {},
                  ident = None,
                  trusted = [],
                  n_concurrent = 1,
                  private_key = None,
                  is_drainable = False,
                  is_also_log_to_stderr = False,
                  mock_actor_handle = None ):
        gevent.Greenlet.__init__( self )

        self.name = uid

        self._log_level = log_level
        self._log_dest = log_dest
        self._beach_config_path = beach_config_path
        self._initLogging( log_level, log_dest )
        self._isAlsoLogToStderr = is_also_log_to_stderr

        self.stopEvent = gevent.event.Event()
        self._realm = realm
        self._ip = ip
        self._port = port
        self._host = host
        self._parameters = parameters
        self._resources = resources
        self._ident = ident
        self._trusted = trusted
        self._n_initial_concurrent = n_concurrent
        self._n_concurrent = 0
        self._private_key = private_key
        self._is_drainable = is_drainable

        self._mock_actor_handle = mock_actor_handle

        self._exception = None

        self._n_free_handlers = 0
        self._is_initted = False

        self._qps = 0.0
        self._q_counter = 0
        self._last_qps_count = 0
        self._q_total_time = 0.0
        self._q_avg = 0.0

        # We have some special magic parameters
        self._trace_enabled = self._parameters.get( 'beach_trace_enabled', False )

        # We keep track of all the handlers for the user per message request type
        self._handlers = {}

        # All user generated threads
        self._threads = gevent.pool.Group()

        # Z values is a generic location to put various environment status
        # and stats relating to the Actor that can be queried in a generic
        # way through the default command 'z' handled by all Actors.
        self._z = {}
        self._z_eps = {}
        self._z_eps_last_summary = time.time()
        self.schedule( 10, self._summarizeEps )

        # This socket receives all taskings for the actor and dispatch
        # the messages as requested by user
        if self._port is None:
            self._opsSocket = _ZMREP( 'ipc:///tmp/test_beach_actor_%s' % ( self.name, ),
                                      isBind = True,
                                      private_key = self._private_key )
        else:
            self._opsSocket = _ZMREP( 'tcp://%s:%d' % ( self._ip, self._port ),
                                      isBind = True,
                                      private_key = self._private_key )

        # This is the local version of the ops socket.
        #self._opsSocketLocal = _ZMREP( 'ipc:///tmp/tmp_lc_actor_sock_%d' % ( self._port, ),
        #                               isBind = True )

        self._vHandles = []
        self.schedule( 10, self._generateQpsCount )
        self.handle( 'z', self._getZValues )

        self.zSet( 'started', int( time.time() ) )
        self.zSet( 'id', self.name )

    def _run( self ):
        try:
            if hasattr( self, 'init' ):
                self.init( self._parameters, self._resources )

            self._is_initted = True

            # Initially Actors handle one concurrent request to avoid bad surprises
            # by users not thinking about concurrency. This can be bumped up by calling
            # Actor.AddConcurrentHandler()
            for i in xrange( self._n_initial_concurrent ):
                self.AddConcurrentHandler()

            self.stopEvent.wait()
        except:
            exc = traceback.format_exc()
            self.logCritical( exc )
            self._exception = exc
        finally:
            self.stop()
            self._opsSocket.close()

            # Before we break the party, we ask gently to exit
            self.log( "Waiting for threads to finish" )
            self._threads.join( timeout = 5 )

            # Finish all the handlers, in theory we could rely on GC to eventually
            # signal the Greenlets to quit, but it's nicer to control the exact timing
            self.log( "Killing any remaining threads" )
            self._threads.kill( timeout = 1 )

            for v in self._vHandles:
                v.close()

            if self._is_initted and hasattr( self, 'deinit' ):
                self.deinit()

    def _getZValues( self, msg ):
        return ( True, self._z )

    def _summarizeEps( self ):
        now = time.time()
        for k, v in self._z_eps.items():
            self.zSet( k, round( v / ( now - self._z_eps_last_summary ), 3 ) )
            self._z_eps[ k ] = 0
        self._z_eps_last_summary = now
        self.zSet( 'n_greenlet', len( self._threads ) )

    def zGet( self, var ):
        '''Get the value of a Z variable.

        :param var: the Z var to get
        '''
        return self._z.get( var, None )

    def zSet( self, var, val ):
        '''Set a Z variable to a specific value.

        :param var: the Z var to set
        :param val: the value to set it to
        '''
        self._z[ var ] = val

    def zInc( self, var, val = 1 ):
        '''Increment the Z variable by one or by optional val.
        
        :param var: the Z var to increment
        :param val: the value increment with, or one by default
        '''
        self._z.setdefault( var, 0 )
        self._z[ var ] += val

    def zIncPerSecond( self, var, val = 1 ):
        '''Increment the Z variable by one or optional val, but generate an average per second.

        :param var: the Z var to increment
        :param val: the value to increment with, or one by default
        '''
        self._z_eps.setdefault( var, 0 )
        self._z_eps[ var ] += val

    def _generateQpsCount( self ):
        now = time.time()
        self._qps = round( self._q_counter / ( now - self._last_qps_count ), 3 )
        self._q_avg = round( self._q_total_time / self._q_counter, 3 ) if 0 != self._q_counter else 0
        self._last_qps_count = now
        self._q_counter = 0
        self._q_total_time = 0.0
        self.zSet( 'qps', self._qps )
        self.zSet( 'avg_q_time', self._q_avg )

    def AddConcurrentHandler( self ):
        '''Add a new thread handling requests to the actor.'''
        self._n_concurrent += 1
        self._threads.add( gevent.spawn( withLogException( self._opsHandler, actor = self ) ) )

    def _opsHandler( self ):
        #z_remote = self._opsSocket.getChild()
        #z_local = self._opsSocketLocal.getChild()
        z = self._opsSocket.getChild()
        isNaturalCleanup = False
        #ready = ZSelector( 10, z_remote, z_local )
        while not self.stopEvent.wait( 0 ):
            self._n_free_handlers += 1
            #z = ready.next()
            #if z is not None:
            #    msg = z.recv( timeout = 10 )
            #else:
            #    msg = False
            msg = z.recv( timeout = 10 )
            self._n_free_handlers -= 1

            if 3 > self._n_free_handlers:
                if 100 > self._n_concurrent:
                    self.AddConcurrentHandler()
                else:
                    # Handlers are full, we start sending back failures
                    # immediately so that callers don't HAVE to way for their
                    # timeout when it's likely it's what will happen.
                    z.send( errorMessage( 'queue full' ) )
                    self.zInc( 'rejected_queue_full' )
                    msg = False

            if msg is False: 
                continue
            start_time = time.time()
            try:
                request = ActorRequest( msg )
            except:
                request = None
            
            self.zInc( 'processed' )

            if not self.stopEvent.wait( 0 ):
                if request is not None:
                    #self.log( "Received: %s" % request.req )

                    if request.dst != self.name:
                        ret = errorMessage( 'wrong dest' )
                        self.log( "Request is for wrong destination from %s, requesting %s but we are %s." % ( request.ident, request.dst, self.name ) )
                        self.zInc( 'bad_dest' )
                    elif ( ( 'z' != request.req ) and ( 0 != len( self._trusted ) ) and ( request.ident not in self._trusted ) ):
                        ret = errorMessage( 'unauthorized' )
                        self.log( "Received unauthorized request from %s." % ( request.ident, ) )
                        self.zInc( 'unauthorized' )
                    else:
                        handler = self._handlers.get( request.req, self._defaultHandler )
                        self._q_counter += 1
                        try:
                            if self._trace_enabled:
                                self.log( 'TRACE::(%s, %s, %s, %s)' % ( request.ident, request.id, request.req, request.dst ) )
                            ret = handler( request )
                        except gevent.GreenletExit:
                            raise
                        except:
                            ret = errorMessage( 'exception', { 'st' : traceback.format_exc() } )
                            self.logCritical( ret )
                            self._exception = ret
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
            #self.log( "Stub call took %s seconds." % ( time.time() - start_time ) )
            self._q_total_time += ( time.time() - start_time )

            # Proactively try to be fair to all handlers.
            self.yieldCpu()

        self.log( "Stopping processing Actor ops requests" )
        #z_remote.close()
        #z_local.close()
        z.close()

    def _defaultHandler( self, msg ):
        self.log( 'request type not supported by actor' )
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

    def getLastError( self ):
        '''Returns the last exception that occured in the actor.

        :returns: an exception or None
        '''
        return self._exception

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

    def unhandle( self, requestType ):
        '''Stop handling requests for a specific type in this actor.
        :param requetType: the string representing the type of request to stop handling
        :returns True if a handler was removed
        '''
        if requestType in self._handlers:
            del( self._handlers[ requestType ] )
            return True
        else:
            return False

    def schedule( self, delay, func, *args, **kw_args ):
        '''Schedule a recurring function.

        :param delay: the number of seconds interval between calls
        :param func: the function to call at interval
        :param args: positional arguments to the function
        :param kw_args: keyword arguments to the function
        '''
        if not self.stopEvent.wait( 0 ):
            func( *args, **kw_args )

            if not self.stopEvent.wait( 0 ):
                self._threads.add( gevent.spawn_later( delay, withLogException( self.schedule, actor = self ), delay, func, *args, **kw_args ) )

    def delay( self, inDelay, func, *args, **kw_args ):
        '''Delay the execution of a function.

        :param inDelay: the number of seconds to execute into
        :param func: the function to call
        :param args: positional arguments to the function
        :param kw_args: keyword arguments to the function
        '''
        self._threads.add( gevent.spawn_later( inDelay, withLogException( func, actor = self ), *args, **kw_args ) )

    def newThread( self, func, *args, **kw_args ):
        '''Starts a function in a new thread. The first argument to the function will be an event signaled when it is time to stop.

        :param func: the function to call
        :param args: positional arguments to the function
        :param kw_args: keyword arguments to the function
        '''
        self._threads.add( gevent.spawn_later( 0, withLogException( func, actor = self ), self.stopEvent, *args, **kw_args ) )

    def parallelExec( self, f, objects, timeout = None ):
        '''Applies a function to N objects in parallel in N threads and waits to return the list results.
        
        :param f: the function to apply
        :param objects: the collection of objects to apply using f
        :param timeouts: number of seconds to wait for results, or None for indefinitely
        '''

        return beach.utils.parallelExec( f, objects, timeout )

    def _initLogging( self, level, dest ):
        self._logger = logging.getLogger( self.name )
        self._logger.setLevel( level )
        handler = logging.handlers.SysLogHandler( address = dest )
        handler.setFormatter( logging.Formatter( "%(asctime)-15s %(message)s" ) )
        self._logger.addHandler( handler )

    def sleep( self, seconds ):
        gevent.sleep( seconds )

    def log( self, msg ):
        '''Log debug statements.

        :param msg: the message to log
        '''
        self._logger.info( '%s : %s', self.__class__.__name__, msg )
        if self._isAlsoLogToStderr:
            err_print( '%s : %s' % ( self.__class__.__name__, msg ) )

    def logCritical( self, msg ):
        '''Log errors.

        :param msg: the message to log
        '''
        self._logger.error( '%s : %s', self.__class__.__name__, msg )
        if self._isAlsoLogToStderr:
            err_print( '%s : %s' % ( self.__class__.__name__, msg ) )

    def getActorHandle( self, category, mode = 'local', nRetries = None, timeout = None ):
        '''Get a virtual handle to actors in the cluster.

        :param category: the name of the category holding actors to get the handle to
        :param mode: the method actors are queried by the handle, currently
            handles: random
        :param nRetries: number of times the handle should attempt to retry the request if
            it times out
        :param timeout: number of seconds to wait before re-issuing a request or failing
        :returns: an ActorHandle
        '''
        if self._mock_actor_handle is None:
            v = ActorHandle( self._realm,
                             category,
                             mode,
                             ident = self._ident,
                             nRetries = nRetries,
                             timeout = timeout,
                             fromActor = self )
            self._vHandles.append( v )
        else:
            v = self._mock_actor_handle( category )
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

    def getActorHandleGroup( self, categoryRoot, mode = 'local', nRetries = None, timeout = None ):
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
                              timeout = timeout,
                              fromActor = self )
        self._vHandles.append( v )
        return v

    def yieldCpu( self ):
        '''Yield the CPU to another coroutine, if needed during long tasks.
        '''
        return gevent.sleep( 0 )

    def getPending( self ):
        '''Get the number of pending requests made by this Actor.
        '''
        return [ h.getPending() for h in self._vHandles ]


class ActorResponse( object ):
    '''Wrapper for responses to requests to Actors. Not created directly.
    Attributes:
    isTimedOut: boolean indicating if the request timed out
    isSuccess: boolean indicating if the request was successful
    error: string with error message if present
    data: data sent back in the response
    '''
    __slots__ = [ 'isTimedOut', 'isSuccess', 'error', 'data' ]

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
            while msg is False:
                iZ = random.randint( 0, len( cls._zDir ) - 1 )
                z = cls._zDir[ iZ ]
                # These requests can be sent to the directory service of a HostManager
                # or the ops service of the HostManager. Directory service is OOB from the
                # ops but is only available locally to Actors. The ops is available from outside
                # the host. So if the ActorHandle is created by an Actor, it goes to the dir_svc
                # and if it's created from outside components through a Beach it goes to
                # the ops.
                msg = z.request( data = { 'req' : 'get_dir', 'realm' : realm, 'cat' : cat }, timeout = 10 )
                if isMessageSuccess( msg ) and 'endpoints' in msg[ 'data' ]:
                    msg = msg[ 'data' ][ 'endpoints' ]
                else:
                    msg = False
                    cls._zDir[ iZ ] = _ZMREQ( cls._zHostDir[ iZ ], isBind = False, private_key = cls._private_key )
                    z.close()
                    gevent.sleep( 1 )
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
                  mode = 'local',
                  nRetries = None,
                  timeout = None,
                  ident = None,
                  fromActor = None ):
        self._cat = category
        self._nRetries = nRetries
        self._timeout = timeout
        self._realm = realm
        self._fromActor = fromActor
        self._mode = mode
        self._ident = ident
        self._endpoints = {}
        self._peerSockets = {}
        self._localSockets = Set()
        self._threads = gevent.pool.Group()
        self._lastDirUpdate = 0
        self._threads.add( gevent.spawn_later( 0, withLogException( self._svc_refreshDir, actor = self._fromActor ) ) )
        self._quick_refresh_timeout = 15
        self._initialRefreshDone = gevent.event.Event()
        self._affinityCache = {}
        self._affinityOrder = None
        self._pending = {}

    def _updateDirectory( self, isForced = False ):
        if not isForced and self._lastDirUpdate > ( time.time() - 2 ):
            return
        else:
            self._lastDirUpdate = time.time()
        newDir = self._getDirectory( self._realm, self._cat )
        if newDir is not False:
            self._endpoints = newDir
            if 'affinity' != self._mode:
                for z_ident, z_url in self._endpoints.items():
                    if z_ident not in self._peerSockets:
                        if self._fromActor is not None and self._fromActor._ip in z_url:
                            # This is on the current box, we can use the IPC optimization.
                            #port = int( z_url.split( ':' )[ -1 ] )
                            #newSocket = _ZMREQ( 'ipc:///tmp/tmp_lc_actor_sock_%d' % ( port, ), isBind = False, congestionCB = self._reportCongestion )
                            newSocket = _ZMREQ( z_url, isBind = False, private_key = self._private_key, congestionCB = self._reportCongestion )
                            self._localSockets.add( z_ident )
                        else:
                            # Do this in two steps since creating a socket is blocking so not thread safe in gevent.
                            newSocket = _ZMREQ( z_url, isBind = False, private_key = self._private_key, congestionCB = self._reportCongestion )
                        if z_ident not in self._peerSockets:
                            self._peerSockets[ z_ident ] = newSocket
                        else:
                            newSocket.close()
            if not self._initialRefreshDone.isSet():
                self._initialRefreshDone.set()

    def _svc_refreshDir( self ):
        self._updateDirectory()
        if ( 0 == len( self._endpoints ) ) and ( 0 < self._quick_refresh_timeout ):
            self._quick_refresh_timeout -= 1
            # No Actors yet, be more agressive to look for some
            self._threads.add( gevent.spawn_later( 2, withLogException( self._svc_refreshDir, actor = self._fromActor ) ) )
        else:
            self._threads.add( gevent.spawn_later( 60 + random.randint( 0, 10 ), withLogException( self._svc_refreshDir, actor = self._fromActor ) ) )

    def _accountedSend( self, z, z_ident, msg, timeout ):
        if z_ident not in self._pending:
            self._pending[ z_ident ] = 0
        self._pending[ z_ident ] += 1

        try:
            ret = z.request( msg, timeout = timeout )
        except:
            ret = { 'status' : { 'success' : False, 'error' : traceback.format_exc() } }
        finally:
            self._pending[ z_ident ] -= 1
            if 0 == self._pending[ z_ident ]:
                del( self._pending[ z_ident ] )
        
        return ret

    def _requestToFuture( self, futureResults, *args, **kwargs ):
        resp = self.request( *args, **kwargs )
        futureResults._addNewResult( resp )

    def request( self, requestType, data = {}, timeout = None, key = None, nRetries = None, isWithFuture = False, onFailure = None ):
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
        :param isWithFuture: return a Future instead of the actual response
        :param onFailure: execute this function callback on failure with a single
            argument that is the message
        :returns: the response to the request as an ActorResponse
        '''
        z = None
        z_ident = None
        ret = False
        curRetry = 0
        affinityKey = None

        # Short-circuit for cases where a category just isn't populated.
        if self._initialRefreshDone.isSet() and 0 == len( self._endpoints ):
            return ActorResponse( False )

        if nRetries is None:
            nRetries = self._nRetries
            if nRetries is None:
                nRetries = 0

        if timeout is None:
            timeout = self._timeout
        if 0 == timeout:
            timeout = None

        if isWithFuture:
            futureResult = FutureResults( 1 )
            self._threads.add( gevent.spawn( withLogException( self._requestToFuture, actor = self._fromActor ), 
                                             futureResult, 
                                             requestType, 
                                             data = data, 
                                             timeout = timeout, 
                                             key = key, 
                                             nRetries = nRetries, 
                                             isWithFuture = False,
                                             onFailure = onFailure ) )
            return futureResult

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
                            if 0 != len( orderedEndpoints ):
                                affinityKey = ( hash( key ) % len( orderedEndpoints ) )
                                if affinityKey in self._affinityCache:
                                    z, z_ident = self._affinityCache[ affinityKey ]
                                else:
                                    z_ident, z = orderedEndpoints[ affinityKey ]
                                    z = _ZMREQ( z, isBind = False, private_key = self._private_key, congestionCB = self._reportCongestion )
                                    self._affinityCache[ affinityKey ] = z, z_ident
                        else:
                            # If we're in a retry, the strategy goes out the window and we just
                            # revert to random.
                            if 'random' == self._mode or curRetry != 0:
                                try:
                                    z_ident = random.choice( self._endpoints.keys() )
                                    z = self._peerSockets[ z_ident ]
                                except:
                                    z = None
                                    z_ident = None
                            elif 'local' == self._mode:
                                try:
                                    z_ident = random.choice( self._localSockets )
                                    z = self._peerSockets[ z_ident ]
                                except:
                                    z_ident = None
                                    z = None
                                if z is None:
                                    try:
                                        z_ident = random.choice( self._endpoints.keys() )
                                        z = self._peerSockets[ z_ident ]
                                    except:
                                        z_ident = None
                                        z = None
                        if z is None:
                            gevent.sleep( 0.1 )
            except _TimeoutException:
                curRetry += 1

            if z is not None and curRetry <= nRetries:
                envelope = { 'data' : data,
                             'mtd' : { 'ident' : self._ident,
                                       'req' : requestType,
                                       'id' : str( uuid.uuid4() ),
                                       'dst' : z_ident } }
                #qStart = time.time()

                ret = self._accountedSend( z, z_ident, envelope, timeout )

                ret = ActorResponse( ret )

                # If we hit a timeout or wrong dest we don't take chances
                # and remove that socket
                if not ret.isTimedOut and ( ret.isSuccess or ( ret.error not in ( 'wrong dest', 'queue full' ) ) ):
                    break
                else:
                    #self._log( "Received failure (%s:%s) after %s: %s" % ( self._cat, requestType, ( time.time() - qStart ), str( ret ) ) )
                    #self._log( "Received failure (%s:%s): %s" % ( self._cat, requestType, str( ret ) ) )
                    if ret.error == 'wrong dest':
                        self._log( "Bad destination, recycling." )
                        # There has been no new response in the last history timeframe, or it's a wrong dest.
                        if 'affinity' == self._mode:
                            self._affinityCache.pop( affinityKey, None )
                        else:
                            self._peerSockets.pop( z_ident, None )
                            self._localSockets.discard( z_ident )
                        z.close()
                    z = None
                    curRetry += 1
                    if ret.error == 'wrong dest':
                        self._updateDirectory()

        if ret is None or ret is False:
            ret = ActorResponse( ret )

        if ret.isTimedOut:
            self._log( "Request failed after %s retries %s:%s." % ( curRetry, self._cat, requestType) )
            if self._fromActor is not None:
                self._fromActor.zInc( 'dropped' )

        if not ret.isSuccess and onFailure is not None:
            onFailure( data )

        return ret

    def _log( self, msg ):
        if self._fromActor is not None:
            self._fromActor.log( msg )
        else:
            syslog.syslog( msg )

    def _reportCongestion( self, isCongested, history ):
        self._log( "Handle %s entering %s state: %s." % ( self._cat, "CONGESTED" if isCongested else "CLEAR", str( history ) ) )

    def broadcast( self, requestType, data = {} ):
        '''Issue a request to all actors in the category of this handle ignoring response.

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

        # Broadcast is inherently very temporal so we assume timeout can be short and without retries.
        self._initialRefreshDone.wait( timeout = 10 )

        for z_ident, z in self._peerSockets.items():
            envelope = copy.deepcopy( envelope )
            envelope[ 'mtd' ][ 'dst' ] = z_ident
            self._threads.add( gevent.spawn( withLogException( self._accountedSend, actor = self._fromActor ), z, z_ident, envelope, 10 ) )

        gevent.sleep( 0 )

        return ret

    def requestFromAll( self, requestType, data = {} ):
        '''Issue a request to all actors in the category of this handle and get responses in a FutureResults.

        :param requestType: the type of request to issue
        :param data: a dict of the data associated with the request
        :returns: True since no validation on the reception or reply from any
            specific endpoint is made
        '''
        envelope = { 'data' : data,
                     'mtd' : { 'ident' : self._ident,
                               'req' : requestType,
                               'id' : str( uuid.uuid4() ) } }

        self._initialRefreshDone.wait( timeout = self._timeout )

        toSockets = self._peerSockets.items()
        futureResults = FutureResults( len( toSockets ) )
        
        for z_ident, z in toSockets:
            envelope = copy.deepcopy( envelope )
            envelope[ 'mtd' ][ 'dst' ] = z_ident
            self._threads.add( gevent.spawn( withLogException( self._directToFuture, actor = self._fromActor ), futureResults, z, z_ident, envelope, self._timeout ) )

        gevent.sleep( 0 )

        return futureResults

    def _directToFuture( self, futureResults, z, z_ident, *args, **kwargs ):
        resp = self._accountedSend( z, z_ident, *args, **kwargs )

        interpretedRet = ActorResponse( resp )
        if not interpretedRet.isSuccess:
            self._log( "Received failure (%s): %s" % ( self._cat, str( interpretedRet ) ) )
            if interpretedRet.error == 'wrong dest' or interpretedRet.isTimedOut:
                self._log( "Bad destination, recycling or timeout." )
                # There has been no new response in the last history timeframe, or it's a wrong dest.
                if 'affinity' != self._mode:
                    self._peerSockets.pop( z_ident, None )
                    self._localSockets.discard( z_ident )
                z.close()
                self._updateDirectory()

        futureResults._addNewResult( interpretedRet )

    def shoot( self, requestType, data = {}, timeout = None, key = None, nRetries = None, onFailure = None ):
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
        :param onFailure: execute this function callback on failure with a single
            argument that is the message
        :returns: True since no validation on the reception or reply
            the endpoint is made
        '''
        ret = True

        if timeout is None:
            timeout = self._timeout

        self._threads.add( gevent.spawn( withLogException( self.request, actor = self._fromActor ), requestType, data, timeout = timeout, key = key, nRetries = nRetries, onFailure = onFailure ) )
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
        if self._fromActor is not None:
            try:
                self._fromActor._vHandles.remove( self )
            except:
                pass
            self._fromActor = None
        self._threads.kill()
        for s in self._peerSockets.values():
            try:
                s.close()
            except:
                pass

    def forceRefresh( self ):
        '''Force a refresh of the handle metadata with nodes in the cluster. This is optional
           since a periodic refresh is already at an interval frequent enough for most use.
        '''
        self._updateDirectory( isForced = True )

    def getPending( self ):
        '''Get the number of pending requests made by this handle.
        '''
        return self._pending

# FutureResults are not meant to be created manually, they are
# generated by calls to ActorHandleGroup.request().
class FutureResults( object ):
    
    __slots__ = [ '_nExpectedResults', '_nReceivedResults', '_newResultEvent', '_isAllReceived', '_results' ]

    def __init__( self, nExpectedResults ):
        self._nExpectedResults = nExpectedResults
        self._nReceivedResults = 0
        self._newResultEvent = gevent.event.Event()
        self._isAllReceived = False
        self._results = []

    def waitForResults( self, timeout = 0 ):
        '''Wait for more results to be available.

        :param timeout: how long to wait for results
        :returns: True if new results are available
        '''
        return self._newResultEvent.wait( timeout = timeout )

    def isFinished( self ):
        '''Checks to see if all results have been received.

        :returns: True if all results have been received
        '''
        return self._isAllReceived

    def _addNewResult( self, res ):
        self._results.append( res )
        self._nReceivedResults += 1
        if self._nExpectedResults <= self._nReceivedResults:
            self._isAllReceived = True
        self._newResultEvent.set()

    def getNewResults( self ):
        '''Get a list of results received from request.
        '''
        ret = self._results
        self._results = []
        self._newResultEvent.clear()
        return ret

class ActorHandleGroup( object ):
    _zHostDir = None
    _zDir = []
    _private_key = None

    def __init__( self,
                  realm,
                  categoryRoot,
                  mode = 'local',
                  nRetries = None,
                  timeout = None,
                  ident = None,
                  fromActor = None ):
        self._realm = realm
        self._categoryRoot = categoryRoot
        if not self._categoryRoot.endswith( '/' ):
            self._categoryRoot += '/'
        self._mode = mode
        self._nRetries = nRetries
        self._timeout = timeout
        self._fromActor = fromActor
        self._ident = ident
        self._quick_refresh_timeout = 15
        self._initialRefreshDone = gevent.event.Event()
        self._threads = gevent.pool.Group()
        self._threads.add( gevent.spawn_later( 0, withLogException( self._svc_periodicRefreshCats, actor = self._fromActor ) ) )
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
            z = random.choice( cls._zDir )
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
        if cats is not None:
            categories = []
            for cat in cats:
                cat = cat.replace( self._categoryRoot, '' ).split( '/' )[ 0 ]
                if 0 == len( cat ):
                    continue
                categories.append( '%s%s/' % ( self._categoryRoot, cat ) )

            for cat in categories:
                if cat not in self._handles:
                    self._handles[ cat ] = ActorHandle( self._realm,
                                                        cat,
                                                        mode = self._mode,
                                                        nRetries = self._nRetries,
                                                        timeout = self._timeout,
                                                        ident = self._ident,
                                                        fromActor = self._fromActor )

            for cat in self._handles.keys():
                if cat not in categories:
                    self._handles[ cat ].close()
                    del( self._handles[ cat ] )
            if not self._initialRefreshDone.isSet():
                self._initialRefreshDone.set()
        return cats

    def _svc_periodicRefreshCats( self ):
        cats = self._refreshCats()
        if cats is False or cats is None or 0 == len( cats ) and 0 < self._quick_refresh_timeout:
            self._quick_refresh_timeout -= 1
            # No Actors yet, be more agressive to look for some
            self._threads.add( gevent.spawn_later( 10, withLogException( self._svc_periodicRefreshCats, actor = self._fromActor ) ) )
        else:
            self._threads.add( gevent.spawn_later( ( 60 * 5 ) + random.randint( 0, 10 ), withLogException( self._svc_periodicRefreshCats, actor = self._fromActor ) ) )

    def shoot( self, requestType, data = {}, timeout = None, key = None, nRetries = None, onFailure = None ):
        '''Send a message to the one actor in each sub-category without waiting for a response.

        :param requestType: the type of request to issue
        :param data: a dict of the data associated with the request
        :param timeout: the number of seconds to wait for a response
        :param nRetries: the number of times the request will be re-sent if it
            times out, meaning a timeout of 5 and a retry of 3 could result in
            a request taking 15 seconds to return
        :param onFailure: execute this function callback on failure with a single
            argument that is the message
        :returns: True since no validation on the reception or reply
            the endpoint is made
        '''
        if timeout is None:
            timeout = self._timeout
        self._initialRefreshDone.wait( timeout = timeout )

        for h in self._handles.values():
            h.shoot( requestType, data, timeout = timeout, key = key, nRetries = nRetries, onFailure = onFailure )

    def request( self, requestType, data = {}, timeout = None, key = None, nRetries = None, onFailure = None ):
        '''Issue a message to the one actor in each sub-category and receive responses asynchronously.

        :param requestType: the type of request to issue
        :param data: a dict of the data associated with the request
        :param timeout: the number of seconds to wait for a response
        :param nRetries: the number of times the request will be re-sent if it
            times out, meaning a timeout of 5 and a retry of 3 could result in
            a request taking 15 seconds to return
        :param onFailure: execute this function callback on failure with a single
            argument that is the message
        :returns: An instance of FutureResults that will receive the responses asynchronously.
        '''
        if timeout is None:
            timeout = self._timeout
        self._initialRefreshDone.wait( timeout = timeout )

        futureResults = FutureResults( len( self._handles ) )

        for h in self._handles.values():
            gevent.spawn( withLogException( self._handleAsyncRequest, actor = self._fromActor ), futureResults, h, requestType, data = data, timeout = timeout, key = key, nRetries = nRetries, onFailure = onFailure )

        return futureResults

    def _handleAsyncRequest( self, futureResults, h, *args, **kwargs ):
        resp = h.request( *args, **kwargs )
        futureResults._addNewResult( resp )

    def getNumAvailable( self ):
        '''Checks to see the number of categories served by this HandleGroup.

        :returns: number of available categories
        '''
        self._initialRefreshDone.wait( 2 )
        return len( self._handles )

    def close( self ):
        '''Close all threads and resources associated with this handle group.
        '''
        self._threads.kill()
        
        for h in self._handles.values():
            try:
                h.close()
            except:
                pass

        if self._fromActor is not None:
            try:
                self._fromActor._vHandles.remove( self )
            except:
                pass
            self._fromActor = None

    def forceRefresh( self ):
        '''Force a refresh of the handle metadata with nodes in the cluster. This is optional
           since a periodic refresh is already at an interval frequent enough for most use.
        '''
        self._refreshCats()
        for handle in self._handles.values():
            handle._updateDirectory( isForced = True )

    def getPending( self ):
        '''Get the number of pending requests made by this handle group.
        '''
        return [ h.getPending() for h in self._handles.values() ]