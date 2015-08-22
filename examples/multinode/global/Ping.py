from beach.actor import Actor
import time
import gevent

class Ping ( Actor ):

    def init( self, parameters ):
        self.log( "Called init of actor." )
        self.zPong = self.getActorHandle( category = 'pongers' )
        self.schedule( 5, self.pinger )

    def deinit( self ):
        self.log( "Called deinit of actor." )

    def pinger( self ):
        if self.zPong.isAvailable():
            self.log( "Sending ping" )
            data = self.zPong.request( 'ping', data = { 'time' : time.time() }, timeout = 10 )
            self.log( "Received pong: %s" % str( data ) )
        else:
            self.log( "No pongers available yet, let's wait a bit" )