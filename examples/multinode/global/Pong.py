from beach.actor import Actor
import time


class Pong ( Actor ):

    def init( self ):
        print( "Called init of actor." )
        self.handle( 'ping', self.ponger )

    def deinit( self ):
        print( "Called deinit of actor." )

    def ponger( self, msg ):
        print( "Received ping: %s" % str( msg ) )
        return { 'time' : time.time() }