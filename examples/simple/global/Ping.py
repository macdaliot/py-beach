from Actor import Actor


class Ping ( Actor ):

    def init( self ):
        print( "Called init of actor." )
        self.schedule( 5, self.pinger )

    def deinit( self ):
        print( "Called deinit of actor." )

    def pinger( self ):
        print( "ping" )