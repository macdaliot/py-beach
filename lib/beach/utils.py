import uuid
import datetime
import gevent
import gevent.coros
import zmq.green as zmq
import netifaces

class TimeoutException(Exception): pass

def sanitizeJson( obj ):
    def _sanitizeJsonValue( value ):
        if type( value ) is uuid.UUID:
            value = str( value )
        elif type( value ) is datetime.datetime:
            value = value.strftime( '%Y-%m-%d %H:%M:%S' )
        return value
    
    def _sanitizeJsonStruct( obj ):
        data = None
        
        if issubclass( type( obj ), dict ):
            data = {}
            for key, value in obj.iteritems():
                try:
                    data[ key ] = _sanitizeJsonValue( value )
                except AttributeError:
                    data[ key ] = _sanitizeValue( value )
        elif issubclass( type( obj ), list ):
            data = []
            for value in obj:
                try:
                    data.append( _sanitizeJsonValue( value ) )
                except AttributeError:
                    data.append( _sanitizeValue( value ) )
        elif type( obj ) is bool:
            data = obj
        else:
            raise AttributeError
                    
        return data
    
    return _sanitizeJsonStruct( obj )

def isMessageSuccess( msg ):
    isSuccess = False
    if msg is not False and msg is not None:
        if msg.get( 'status', {} ).get( 'success', False ) is not False:
            isSuccess = True
    return isSuccess

def errorMessage( errorString, data = None ):
    msg = { 'status' : { 'success' : False, 'error' : errorString } }
    if data is not None:
        msg[ 'data' ] = data
    return msg

def successMessage( data = None ):
    msg = { 'status' : { 'success' : True } }
    if data is not None:
        msg.update( data )
    return msg

class ZSocket( object ):
    
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
                with gevent.Timeout( timeout, TimeoutException ):
                    self.s.send_json( sanitizeJson( data ) )
            else:
                self.s.send_json( sanitizeJson( data ) )
        except TimeoutException:
            self._rebuildIfNecessary()
        except zmq.ZMQError, e:
            raise
        else:
            isSuccess = True

        return isSuccess
    
    def recv( self, timeout = None ):
        data = False

        try:
            if timeout is not None:
                with gevent.Timeout( timeout, TimeoutException ):
                    data = self.s.recv_json()
            else:
                data = self.s.recv_json()
        except TimeoutException:
            self._rebuildIfNecessary()
        except zmq.ZMQError, e:
            raise

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

class ZMREQ ( object ):
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
            with gevent.Timeout( timeout, TimeoutException ):
                z.send_json( sanitizeJson( data ) )
                result = z.recv_json()
        except TimeoutException:
            z.close( linger = 0 )
            z = self._newSocket()

        self._available.append( z )

        return result

class ZMREP ( object ):
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
        def __init__( self, z ):
            self._z = z

        def send( self, data, timeout = None ):
            isSuccess = False

            try:
                if timeout is not None:
                    with gevent.Timeout( timeout, TimeoutException ):
                        self._z.send_json( sanitizeJson( data ) )
                else:
                    self._z.send_json( sanitizeJson( data ) )
            except TimeoutException:
                isSuccess = False
            except zmq.ZMQError, e:
                raise
            else:
                isSuccess = True

            return isSuccess

        def recv( self, timeout = None ):
            data = False

            try:
                if timeout is not None:
                    with gevent.Timeout( timeout, TimeoutException ):
                        data = self._z.recv_json()
                else:
                    data = self._z.recv_json()
            except TimeoutException:
                data = False
            except zmq.ZMQError, e:
                raise

            return data

    def getChild( self ):
        return self._childSock( self._newSocket() )

    def _newSocket( self ):
        z = self._ctx.socket( zmq.REP )
        z.set( zmq.LINGER, 0 )
        z.connect( self._intUrl )
        return z

    def _proxy( self, zFrom, zTo ):
        while True:
            msg = zFrom.recv_multipart()
            zTo.send_multipart( msg )


def getIpv4ForIface( iface ):
    ip = None
    try:
        ip = netifaces.ifaddresses( iface )[ netifaces.AF_INET ][ 0 ][ 'addr' ]
    except:
        pass
    return ip