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
import gevent.coros
import zmq.green as zmq
import netifaces
from prefixtree import PrefixDict
import msgpack

class _TimeoutException(Exception): pass

def _sanitizeJson( obj ):
    def _sanitizeJsonValue( value ):
        if type( value ) is uuid.UUID:
            value = str( value )
        elif type( value ) is datetime.datetime:
            value = value.strftime( '%Y-%m-%d %H:%M:%S' )
        elif type( value ) not in ( str, unicode, bool, int, float ):
            value = str( value )
        return value
    
    def _sanitizeJsonStruct( obj ):
        if issubclass( type( obj ), dict ) or type( obj ) is PrefixDict:
            data = {}
            for key, value in obj.iteritems():
                try:
                    data[ key ] = _sanitizeJsonStruct( value )
                except AttributeError:
                    data[ key ] = _sanitizeJsonValue( value )
        elif issubclass( type( obj ), list ) or issubclass( type( obj ), tuple ):
            data = []
            for value in obj:
                try:
                    data.append( _sanitizeJsonStruct( value ) )
                except AttributeError:
                    data.append( _sanitizeJsonValue( value ) )
        elif type( obj ) is bool:
            data = obj
        else:
            raise AttributeError
                    
        return data
    
    return _sanitizeJsonStruct( obj )

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
    
    def __init__( self, socketType, url, isBind = False ):
        self.ctx = zmq.Context()
        self._socketType = socketType
        self._url = url
        self._isBind = isBind
        self._isTransactionSocket = ( self._socketType == zmq.REQ or self._socketType == zmq.REP )

        self._buildSocket()

    def _buildSocket( self ):
        self.s = self.ctx.socket( self._socketType )
        self.s.set( zmq.LINGER, 0 )
        if self._isBind:
            self.s.bind( self._url )
        else:
            self.s.connect( self._url )
        if self._isTransactionSocket:
            self._lock = gevent.coros.BoundedSemaphore( 1 )

    def _rebuildIfNecessary( self ):
        if self._isTransactionSocket:
            self._buildSocket()
    
    def send( self, data, timeout = None ):
        isSuccess = False

        try:
            if timeout is not None:
                with gevent.Timeout( timeout, _TimeoutException ):
                    self.s.send( msgpack.packb( _sanitizeJson( data ) ) )
            else:
                self.s.send( msgpack.packb( _sanitizeJson( data ) ) )
        except _TimeoutException:
            self._rebuildIfNecessary()
        except zmq.ZMQError, e:
            self._buildSocket()
        else:
            isSuccess = True

        return isSuccess
    
    def recv( self, timeout = None ):
        data = False

        try:
            if timeout is not None:
                with gevent.Timeout( timeout, _TimeoutException ):
                    data = msgpack.unpackb( self.s.recv(), use_list = True )
            else:
                data = msgpack.unpackb( self.s.recv(), use_list = True )
        except _TimeoutException:
            self._rebuildIfNecessary()
        except zmq.ZMQError, e:
            self._buildSocket()

        return data
    
    def request( self, data, timeout = None ):
        self._lock.acquire()
        self.send( data, timeout )
        data = self.recv( timeout = timeout )
        # False indicates a timeout or failure, where the socket
        # would have been rebuilt
        if data is not False or not self._isTransactionSocket:
            self._lock.release()
        return data

    def close( self ):
        self.s.close()

class _ZMREQ ( object ):
    def __init__( self, url, isBind ):
        self._available = []
        self._url = url
        self._isBind = isBind
        self._ctx = zmq.Context()

    def _newSocket( self ):
        z = self._ctx.socket( zmq.REQ )
        z.set( zmq.LINGER, 0 )
        if self._isBind:
            z.bind( self._url )
        else:
            z.connect( self._url )
        return z

    def request( self, data, timeout = None ):
        result = False
        z = None

        try:
            z = self._available.pop()
        except:
            z = self._newSocket()

        try:
            with gevent.Timeout( timeout, _TimeoutException ):
                z.send( msgpack.packb( _sanitizeJson( data ) ) )
                result = msgpack.unpackb( z.recv(), use_list = True )
        except _TimeoutException:
            z.close( linger = 0 )
            z = self._newSocket()
        except zmq.ZMQError, e:
            z = self._newSocket()

        self._available.append( z )

        return result

class _ZMREP ( object ):
    def __init__( self, url, isBind ):
        self._available = []
        self._url = url
        self._isBind = isBind
        self._ctx = zmq.Context()
        self._threads = gevent.pool.Group()
        self._intUrl = 'inproc://%s' % str( uuid.uuid4() )

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
        def __init__( self, z_func ):
            self._z_func = z_func
            self._buildSocket()

        def _buildSocket( self ):
            self._z = self._z_func()

        def send( self, data, timeout = None ):
            isSuccess = False

            try:
                if timeout is not None:
                    with gevent.Timeout( timeout, _TimeoutException ):
                        self._z.send( msgpack.packb( _sanitizeJson( data ) ) )
                else:
                    self._z.send( msgpack.packb( _sanitizeJson( data ) ) )
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
                        data = msgpack.unpackb( self._z.recv(), use_list = True )
                else:
                    data = msgpack.unpackb( self._z.recv(), use_list = True )
            except _TimeoutException:
                data = False
            except zmq.ZMQError, e:
                self._z.close( linger = 0 )
                self._buildSocket()

            return data

    def getChild( self ):
        return self._childSock( self._newSocket )

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