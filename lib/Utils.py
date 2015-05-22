import uuid
import datetime
import gevent
import zmq.green as zmq
import netifaces

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

    class TimeoutException(Exception): pass
    
    def __init__( self, socketType, url, isBind = False ):
        self.ctx = zmq.Context()
        self._socketType = socketType
        self._url = url
        self._isBind = isBind

        self._buildSocket()

    def _buildSocket( self ):
        self.s = self.ctx.socket( self._socketType )
        if self._isBind:
            self.s.bind( self._url )
        else:
            self.s.connect( self._url )

    def _rebuildIfNecessary( self ):
        if self._socketType == zmq.REQ or self._socketType == zmq.REP:
            self._buildSocket()
    
    def send( self, data, timeout = None ):
        isSuccess = False
        
        try:
            if timeout is not None:
                with gevent.Timeout( timeout, self.TimeoutException ):
                    self.s.send_json( sanitizeJson( data ) )
            else:
                self.s.send_json( sanitizeJson( data ) )
        except self.TimeoutException:
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
                with gevent.Timeout( timeout, self.TimeoutException ):
                    data = self.s.recv_json()
            else:
                data = self.s.recv_json()
        except self.TimeoutException:
            self._rebuildIfNecessary()
        except zmq.ZMQError, e:
            raise
        
        return data
    
    def request( self, data, timeout = None ):
        self.send( data, timeout )
        return self.recv( timeout = timeout )

def getIpv4ForIface( iface ):
    ip = None
    try:
        ip = netifaces.ifaddresses( iface )[ netifaces.AF_INET ][ 0 ][ 'addr' ]
    except:
        pass
    return ip