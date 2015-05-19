import uuid
import datetime
import psycopg2.extras
import zmq.green as zmq

def sanitizeJson( obj ):
    def _sanitizeJsonValue( value ):
        if type( value ) is uuid.UUID:
            value = str( value )
        elif type( value ) is datetime.datetime:
            value = value.strftime( '%Y-%m-%d %H:%M:%S' )
        return value
    
    def _sanitizeJsonStruct( obj ):
        data = None
        
        if type( obj ) is psycopg2.extras.DictRow:
            obj = dict( obj )
        
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
    if msg is not False:
        if msg.get( 'status', {} ).get( 'success', False ) is not False:
            isSuccess = True
    return isSuccess

def errorMessage( errorString, data = None ):
    msg = { 'status' : False, 'error' : errorString }
    if data is not None:
        msg[ 'data' ] = data
    return msg

def successMessage( data = None ):
    msg = { 'status' : True }
    if data is not None:
        msg.update( data )
    return msg

class ZSocket( object ):
    
    def __init__( self, socketType, url, isBind = False ):
        self.ctx = zmq.Context()
        self.s = self.ctx.socket( socketType, url )
        if isBind:
            self.s.bind( url )
        else:
            self.s.connect( url )
    
    def send( self, data ):
        isSuccess = False
        
        try:
            self.s.send_json( sanitizeJson( data ) )
        except zmq.ZMQError, e:
            pass
        else:
            isSuccess = True
        
        return isSuccess
    
    def recv( self ):
        data = False
        
        try:
            data = self.s.recv_json()
        except zmq.ZMQError, e:
            pass
        
        return data
    
    def request( self, data ):
        self.send( data )
        return self.recv()