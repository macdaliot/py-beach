from Actor import Actor
import time

class Ping ( Actor ):

    def init( self ):
        print( "Called init of actor." )
        self.zPong = self.getActorHandle( category = 'pongers' )
        self.schedule( 5, self.pinger )

    def deinit( self ):
        print( "Called deinit of actor." )

    def pinger( self ):
        print( "Sending ping" )
        data = self.zPong.request( 'ping', data = { 'time' : time.time() }, timeout = 10 )
        print( "Received pong: %s" % str( data ) )