from beach.actor import Actor
import time


class Pong ( Actor ):

    def init( self, parameters, resources ):
        self.log( "Called init of actor." )
        self.handle( 'ping', self.ponger )

    def deinit( self ):
        self.log( "Called deinit of actor." )

    def ponger( self, msg ):
        self.log( "Received ping: %s" % str( msg ) )
        return ( True, { 'time' : time.time() } )