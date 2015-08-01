from beach.actor import Actor
import time
import gevent

class Ping ( Actor ):

    def init( self, parameters ):
        print( "Called init of actor." )
        self.zPong = self.getActorHandle( category = 'pongers' )
        self.schedule( 5, self.pinger )

    def deinit( self ):
        print( "Called deinit of actor." )

    def pinger( self ):
        if self.zPong.isAvailable():
            print( "Sending ping" )
            data = self.zPong.request( 'ping', data = { 'time' : time.time() }, timeout = 10 )
            print( "Received pong: %s" % str( data ) )
        else:
            print( "No pongers available yet, let's wait a bit" )