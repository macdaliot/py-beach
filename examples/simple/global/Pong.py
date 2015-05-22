from Actor import Actor


class Pong ( Actor ):

    def init( self ):
        print( "Called init of actor." )
        self.schedule( 5, self.ponger )

    def deinit( self ):
        print( "Called deinit of actor." )

    def ponger( self ):
        print( "pong" )