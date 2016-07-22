from beach.actor import Actor
import time


class Sleeper ( Actor ):

    def init( self, parameters, resources ):
        self.log( "Called init of actor." )
        self.handle( 'sleep', self.doSleep )
        self.handle( 'nosleep', self.noSleep )

    def deinit( self ):
        self.log( "Called deinit of actor." )

    def doSleep( self, msg ):
        self.log( "Received sleep: %s" % str( msg ) )
        self.sleep( 10 )
        return ( True, { 'time' : time.time() } )

    def noSleep( self, msg ):
        self.log( "Received nosleep: %s" % str( msg ) )
        return ( True, { 'time' : time.time() } )