from beach.actor import Actor
import time


class MultiPing ( Actor ):

    def init( self, parameters, resources ):
        self.log( "Called init of actor." )
        self.handle( 'ping', self.ponger )
        self.handle( 'oob', self.oobHandler )

    def deinit( self ):
        self.log( "Called deinit of actor." )

    def ponger( self, msg ):
        self.log( "Received ping: %s" % str( msg ) )
        return ( True, { 'time' : time.time() } )

    def oobHandler( self, msg ):
        self.log( "Received oob: %s" % str( msg ) )
        return ( True, { 'cmdresult' : 42 } )