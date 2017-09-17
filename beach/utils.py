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

import uuid
import datetime
import gevent
from gevent.lock import BoundedSemaphore
from gevent.event import Event
import gevent.queue
import zmq.green as zmq
import netifaces
from prefixtree import PrefixDict
import msgpack
import hashlib
import imp
import urllib2
import sys
import os
import warnings
import time
try:
    import M2Crypto
    IV_LENGTH = 0x10
except:
    print( "Beach crypto facilities disable due to failure to load M2Crypto." )

class _TimeoutException(Exception): pass

global_z_context = zmq.Context()
global_z_context.set( zmq.MAX_SOCKETS, 1024 * 10 )

def loadModuleFrom( path, realm ):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        if path.startswith( 'file://' ):
            path = 'file://%s' % os.path.abspath( path[ 7 : ] )
        name = path[ path.rfind( '/' ) + 1 : ]
        name = name if not name.endswith( '.py' ) else name[ : -3 ]
        hUrl = urllib2.urlopen( path )
        content = hUrl.read()
        hUrl.close()
        modHash = hashlib.sha1( content ).hexdigest()
        name = '%s_%s' % ( name, modHash )
        mod = sys.modules.get( name, None )
        if mod is None:
            mod = imp.new_module( name )
            mod.__dict__[ '_beach_path' ] = path
            mod.__dict__[ '__file__' ] = path
            mod.__dict__[ '_beach_realm' ] = realm
            exec( content, mod.__dict__ )
            sys.modules[ name ] = mod
    
    return mod

def _retExecOrExc( f, o, r ):
    try:
        r.append( f( o ) )
    except Exception as e:
        r.append( e )

def parallelExec( f, objects, timeout = None ):
    g = gevent.pool.Group()
    results = []
    for o in objects:
        g.add( gevent.spawn_later( 0, _retExecOrExc, f, o, results ) )
    g.join( timeout = timeout )
    return results

def _sanitizeData( obj ):
    if isinstance( obj, datetime.datetime ):
        return obj.strftime( '%Y-%m-%d %H:%M:%S' )
    elif isinstance( obj, PrefixDict ):
        return dict( obj )
    else:
        return str( obj )

def isMessageSuccess( msg ):
    '''Checks if request was a success.
    
    :param msg: the message to check for success
    :returns: True if it was a success
    '''
    isSuccess = False
    if msg is not False and msg is not None:
        if msg.get( 'status', {} ).get( 'success', False ) is not False:
            isSuccess = True
    return isSuccess

def errorMessage( errorString, data = None ):
    '''Create a generic error message.

    :param errorString: the error message to associate
    :param data: specific data to contextualize the error
    :returns: an error message that can be sent back to any actor
    '''
    msg = { 'status' : { 'success' : False, 'error' : errorString } }
    if data is not None:
        msg[ 'data' ] = data
    return msg

def successMessage( data = None ):
    '''Create a generic success message.

    :param data: specific data returned in the success message
    :returns: the success message
    '''
    msg = { 'status' : { 'success' : True } }
    if data is not None:
        msg[ 'data' ] = data
    return msg

class _ZSocket( object ):
    
    def __init__( self, socketType, url, isBind = False, private_key = None ):
        global global_z_context
        self.ctx = global_z_context
        self.s = None
        self._socketType = socketType
        self._url = url
        self._isBind = isBind
        self._private_key = private_key
        self._isTransactionSocket = ( self._socketType == zmq.REQ or self._socketType == zmq.REP )

        self._buildSocket()

    def _buildSocket( self ):
        if self.s is not None:
            self.s.close( linger = 0 )
            self.s = None
        self.s = self.ctx.socket( self._socketType )
        self.s.set( zmq.LINGER, 0 )
        if self._isBind:
            self.s.bind( self._url )
        else:
            self.s.connect( self._url )
        if self._isTransactionSocket:
            self._lock = BoundedSemaphore( 1 )

    def _rebuildIfNecessary( self ):
        if self._isTransactionSocket:
            self._buildSocket()
    
    def send( self, data, timeout = None ):
        isSuccess = False

        # If socket is None it means someone closed it.
        if self.s is None: return False

        data = msgpack.packb( data, default = _sanitizeData )

        if self._private_key is not None:
            sym_iv = M2Crypto.Rand.rand_bytes( IV_LENGTH )
            aes = M2Crypto.EVP.Cipher( alg = 'aes_256_cbc',
                                       key = self._private_key,
                                       iv = sym_iv,
                                       salt = False,
                                       key_as_bytes = False,
                                       op = 1 ) # 1 == ENC
            data = sym_iv + aes.update( data )
            data += aes.final()

        try:
            if timeout is not None:
                with gevent.Timeout( timeout, _TimeoutException ):
                    self.s.send( data )
            else:
                self.s.send( data )
        except _TimeoutException:
            self._rebuildIfNecessary()
        except zmq.ZMQError, e:
            self._buildSocket()
        except Exception, e:
            self._buildSocket()
        else:
            isSuccess = True

        return isSuccess
    
    def recv( self, timeout = None ):
        data = False

        # If socket is None it means someone closed it.
        if self.s is None: return False

        try:
            if timeout is not None:
                with gevent.Timeout( timeout, _TimeoutException ):
                    data = self.s.recv()
            else:
                data = self.s.recv()
        except _TimeoutException:
            self._rebuildIfNecessary()
        except zmq.ZMQError, e:
            self._buildSocket()

        if data is not None and data is not False:
            if self._private_key is not None:
                if IV_LENGTH < len( data ):
                    sym_iv = data[ 0 : IV_LENGTH ]
                    data = data[ IV_LENGTH : ]
                    aes = M2Crypto.EVP.Cipher( alg = 'aes_256_cbc',
                                               key = self._private_key,
                                               iv = sym_iv,
                                               salt = False,
                                               key_as_bytes = False,
                                               op = 0 ) # 0 == DEC
                    data = aes.update( data )
                    data += aes.final()
                else:
                    data = None

            if data is not None:
                data = msgpack.unpackb( data, use_list = False )

        return data
    
    def request( self, data, timeout = None ):
        self._lock.acquire()
        # If socket is None it means someone closed it.
        if self.s is None: return False

        if self.send( data, timeout ):
            data = self.recv( timeout = timeout )
        else:
            data = False
        # False indicates a timeout or failure, where the socket
        # would have been rebuilt
        if data is not False or not self._isTransactionSocket:
            self._lock.release()
        return data

    def close( self ):
        self.s.close( linger = 0 )
        self.s = None

class _ZMREQ ( object ):
    def __init__( self, url, isBind, private_key = None, congestionCB = None ):
        global global_z_context
        self._available = gevent.queue.Queue()
        self._isClosed = False
        self._url = url
        self._isBind = isBind
        self._ctx = global_z_context
        self._threads = gevent.pool.Group()
        self._intUrl = 'inproc://%s' % str( uuid.uuid4() )
        self._private_key = private_key
        self.isCongested = False
        self._outQ = 0.0
        self._inQ = 0.0
        self.growthHist = [ 1.0, 1.0, 1.0, 1.0 ]
        self._congestionCB = congestionCB

        zFront = self._ctx.socket( zmq.ROUTER )
        zBack = self._ctx.socket( zmq.DEALER )
        zFront.set( zmq.LINGER, 0 )
        zBack.set( zmq.LINGER, 0 )
        if self._isBind:
            zBack.bind( self._url )
        else:
            zBack.connect( self._url )
        zFront.bind( self._intUrl )
        self._proxySocks = ( zFront, zBack )
        self._threads.add( gevent.spawn( self._proxy, zFront, zBack ) )
        self._threads.add( gevent.spawn( self._proxy, zBack, zFront ) )
        self._threads.add( gevent.spawn_later( 5, self._updateStats ) )

    def _proxy( self, zFrom, zTo ):
        while True:
            msg = zFrom.recv_multipart()
            zTo.send_multipart( msg )

    def _updateStats( self ):
        inQ = self._inQ
        outQ = self._outQ
        if 0 == inQ:
            if 0 == outQ:
                growth = 0
            else:
                growth = 999
        else:
            growth = float( outQ ) / float( inQ )
        self._outQ = 0.0
        self._inQ = 0.0
        self.growthHist.insert( 0, growth )
        self.growthHist.pop()
        if ( ( 1.05 < self.growthHist[ 0 ] and 1.05 < self.growthHist[ 1 ] and 999 != self.growthHist[ 0 ] and 999 != self.growthHist[ 1 ] ) or
             ( 1.0 < self.growthHist[ 0 ] and 1.0 < self.growthHist[ 1 ] and 1.0 < self.growthHist[ 2 ] and 1.0 < self.growthHist[ 3 ] and 
               999 != self.growthHist[ 0 ] and 999 != self.growthHist[ 1 ] and 999 != self.growthHist[ 2 ] and 999 != self.growthHist[ 3 ] ) ):
            if not self.isCongested:
                self.isCongested = True
                if self._congestionCB is not None:
                    self._congestionCB( True, self.growthHist )
        elif 100 > self._available.qsize():
            if self.isCongested and 1.0 > self.growthHist[ 0 ]:
                self.isCongested = False
                if self._congestionCB is not None:
                    self._congestionCB( False, self.growthHist )
        self._threads.add( gevent.spawn_later( 5, self._updateStats ) )

    def _newSocket( self ):
        try:
            z = self._ctx.socket( zmq.REQ )
            z.set( zmq.LINGER, 0 )
            z.connect( self._intUrl )
        except zmq.ZMQError as e:
            if 'Too many open files' in str( e ):
                z = None
            else:
                raise
        return z

    def request( self, data, timeout = None ):
        result = False
        z = None
        try:
            z = self._available.get( block = False )
        except:
            if not self.isCongested:
                z = self._newSocket()
                if z is None:
                    self.isCongested = True
                    if self._congestionCB is not None:
                        self._congestionCB( True, self.growthHist )

            if z is None:
                try:
                    z = self._available.get( block = True, timeout = timeout ) 
                except gevent.queue.Empty:
                    return False

        ts = time.time()
        try:
            with gevent.Timeout( timeout, _TimeoutException ):
                data = msgpack.packb( data, default = _sanitizeData )

                if self._private_key is not None:
                    sym_iv = M2Crypto.Rand.rand_bytes( IV_LENGTH )
                    aes = M2Crypto.EVP.Cipher( alg = 'aes_256_cbc',
                                               key = self._private_key,
                                               iv = sym_iv,
                                               salt = False,
                                               key_as_bytes = False,
                                               op = 1 ) # 1 == ENC
                    data = sym_iv + aes.update( data )
                    data += aes.final()

                self._outQ += 1
                try:
                    z.send( data )
                    result = z.recv()
                finally:
                    self._inQ += 1

                if result is not None and result is not False:
                    if self._private_key is not None:
                        if IV_LENGTH < len( result ):
                            sym_iv = result[ 0 : IV_LENGTH ]
                            result = result[ IV_LENGTH : ]
                            aes = M2Crypto.EVP.Cipher( alg = 'aes_256_cbc',
                                                       key = self._private_key,
                                                       iv = sym_iv,
                                                       salt = False,
                                                       key_as_bytes = False,
                                                       op = 0 ) # 0 == DEC
                            result = aes.update( result )
                            result += aes.final()
                        else:
                            result = None

                    if result is not None:
                        result = msgpack.unpackb( result, use_list = False )

        except _TimeoutException:
            z.close( linger = 0 )
            z = self._newSocket()
            result = False
        except zmq.ZMQError:
            z.close( linger = 0 )
            z = self._newSocket()
            result = False

        if z is not None:
            if self._isClosed or result is False or result is None:
                z.close( linger = 0 )
            else:
                if 10 < self._available.qsize():
                    z.close( linger = 0 )
                else:
                    self._available.put( z )
        return result

    def close( self ):
        self._isClosed = True
        while not self._available.empty():
            try:
                z = self._available.get( block = False )
                z.close( linger = 0 )
            except:
                break
        self._available = gevent.queue.Queue()
        self._threads.kill()
        if self._proxySocks[ 0 ] is not None:
            self._proxySocks[ 0 ].close()
        if self._proxySocks[ 1 ] is not None:
            self._proxySocks[ 1 ].close()
        self._proxySocks = ( None, None )

class _ZMREP ( object ):
    def __init__( self, url, isBind, private_key = None ):
        global global_z_context
        self._url = url
        self._isBind = isBind
        self._ctx = global_z_context
        self._threads = gevent.pool.Group()
        self._intUrl = 'inproc://%s' % str( uuid.uuid4() )
        self._private_key = private_key

        zFront = self._ctx.socket( zmq.ROUTER )
        zBack = self._ctx.socket( zmq.DEALER )
        zFront.set( zmq.LINGER, 0 )
        zBack.set( zmq.LINGER, 0 )
        if self._isBind:
            zFront.bind( self._url )
        else:
            zFront.connect( self._url )
        zBack.bind( self._intUrl )
        self._proxySocks = ( zFront, zBack )
        self._threads.add( gevent.spawn( self._proxy, zFront, zBack ) )
        self._threads.add( gevent.spawn( self._proxy, zBack, zFront ) )

    def close( self ):
        self._threads.kill()
        self._proxySocks[ 0 ].close()
        self._proxySocks[ 1 ].close()
        self._proxySocks = ( None, None )

    class _childSock( object ):
        __slots__ = [ '_z_func', '_z', '_private_key' ]

        def __init__( self, z_func, private_key = None ):
            self._z_func = z_func
            self._z = None
            self._buildSocket()
            self._private_key = private_key

        def _buildSocket( self ):
            if self._z is not None:
                self._z.close()
            self._z = self._z_func()

        def close( self ):
            if self._z is not None:
                self._z.close()
                self._z = None

        def send( self, data, timeout = None, isSkipSanitization = False ):
            isSuccess = False

            if isSkipSanitization:
                data = msgpack.packb( data )
            else:
                data = msgpack.packb( data, default = _sanitizeData )

            if self._private_key is not None:
                sym_iv = M2Crypto.Rand.rand_bytes( IV_LENGTH )
                aes = M2Crypto.EVP.Cipher( alg = 'aes_256_cbc',
                                           key = self._private_key,
                                           iv = sym_iv,
                                           salt = False,
                                           key_as_bytes = False,
                                           op = 1 ) # 1 == ENC
                data = sym_iv + aes.update( data )
                data += aes.final()

            try:
                if timeout is not None:
                    with gevent.Timeout( timeout, _TimeoutException ):
                        self._z.send( data )
                else:
                    self._z.send( data )
            except _TimeoutException:
                self._z.close( linger = 0 )
                self._buildSocket()
            except zmq.ZMQError, e:
                self._z.close( linger = 0 )
                self._buildSocket()
            else:
                isSuccess = True

            return isSuccess

        def recv( self, timeout = None ):
            data = False
            try:
                if timeout is not None:
                    with gevent.Timeout( timeout, _TimeoutException ):
                        data = self._z.recv()
                else:
                    data = self._z.recv()
            except _TimeoutException:
                data = False
            except zmq.ZMQError, e:
                self._z.close( linger = 0 )
                self._buildSocket()

            if data is not None and data is not False:
                if self._private_key is not None:
                    if IV_LENGTH < len( data ):
                        sym_iv = data[ 0 : IV_LENGTH ]
                        data = data[ IV_LENGTH : ]
                        aes = M2Crypto.EVP.Cipher( alg = 'aes_256_cbc',
                                                   key = self._private_key,
                                                   iv = sym_iv,
                                                   salt = False,
                                                   key_as_bytes = False,
                                                   op = 0 ) # 0 == DEC
                        data = aes.update( data )
                        data += aes.final()
                    else:
                        data = None

                if data is not None:
                    data = msgpack.unpackb( data, use_list = False )
            return data

    def getChild( self ):
        return self._childSock( self._newSocket, private_key = self._private_key )

    def _newSocket( self ):
        z = self._ctx.socket( zmq.REP )
        z.set( zmq.LINGER, 0 )
        z.connect( self._intUrl )
        return z

    def _proxy( self, zFrom, zTo ):
        while True:
            msg = zFrom.recv_multipart()
            zTo.send_multipart( msg )


def _getIpv4ForIface( iface ):
    ip = None
    try:
        ip = netifaces.ifaddresses( iface )[ netifaces.AF_INET ][ 0 ][ 'addr' ]
    except:
        pass
    return ip

def _getPublicInterfaces():
    interfaces = []

    for iface in netifaces.interfaces():
        ipv4s = netifaces.ifaddresses( iface ).get( netifaces.AF_INET, [] )
        for entry in ipv4s:
            addr = entry.get( 'addr' )
            if not addr:
                continue
            if not ( iface.startswith( 'lo' ) or addr.startswith( '127.' ) ):
                interfaces.append( iface )
                break

    return interfaces

class _rwlock_w( object ):
    def __init__( self, l ):
        self.l = l
    def __enter__( self ):
        self.l.wLock()
        return self
    def __exit__( self, type, value, traceback ):
        self.l.wUnlock()

class _rwlock_r( object ):
    def __init__( self, l ):
        self.l = l
    def __enter__( self ):
        self.l.rLock()
        return self
    def __exit__( self, type, value, traceback ):
        self.l.rUnlock()

class RWLock( object ):
    def __init__( self ):
        self._canRead = Event()
        self._canWrite = Event()
        self._mutex = BoundedSemaphore( value = 1 )
        self._readers = 0
        self._isWriting = False
        self._canRead.set()
        self._canWrite.set()

    def rLock( self ):
        isReady = False
        while not isReady:
            self._canRead.wait()
            self._mutex.acquire( blocking = True, timeout = None )
            if not self._isWriting:
                self._canWrite.clear()
                self._readers += 1
                isReady = True
            self._mutex.release()

    def rUnlock( self ):
        self._mutex.acquire( blocking = True, timeout = None )
        self._readers -= 1
        if 0 == self._readers:
            self._canWrite.set()
        self._mutex.release()

    def wLock( self ):
        isReady = False
        while not isReady:
            self._canRead.clear()
            self._canWrite.wait()
            self._mutex.acquire( blocking = True, timeout = None )
            if not self._isWriting and 0 == self._readers:
                isReady = True
                self._isWriting = True
                self._canWrite.clear()
            self._mutex.release()

    def wUnlock( self ):
        self._mutex.acquire( blocking = True, timeout = None )
        self._isWriting = False
        self._canWrite.set()
        self._canRead.set()
        self._mutex.release()

    def writer( self ):
        return _rwlock_w( self )

    def reader( self ):
        return _rwlock_r( self )


import inspect

class Counter( object ):
    def __init__( self ):
        self.counters = {}

    def count( self ):
        ( frame, filename, line_number, function_name, lines, index ) = inspect.getouterframes( inspect.currentframe() )[ 1 ]
        self.counters.setdefault( line_number, 0 )
        self.counters[ line_number ] += 1
