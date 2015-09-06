import os
import sys

g_current_dir = os.path.dirname( os.path.abspath( __file__ ) )

from beach.beach_api import Beach

import traceback
import web
import datetime
import time
import json
from functools import wraps


###############################################################################
# CUSTOM EXCEPTIONS
###############################################################################


###############################################################################
# REFERENCE ELEMENTS
###############################################################################


###############################################################################
# CORE HELPER FUNCTIONS
###############################################################################
def tsToTime( ts ):
    return datetime.datetime.fromtimestamp( int( ts ) ).strftime( '%Y-%m-%d %H:%M:%S' )

###############################################################################
# PAGE DECORATORS
###############################################################################


###############################################################################
# PAGES
###############################################################################
class Index:
    def GET( self ):
        return render.index()

class GetClusterInfo:
    def GET( self ):
        info = {}
        web.header( 'Content-Type', 'application/json' )
        info[ 'dir' ] = beach.getDirectory()
        info[ 'health' ] = beach.getClusterHealth()
        info[ 'n_nodes' ] = beach.getNodeCount()

        n_actors = 0
        n_realms = 0
        n_cats = 0
        for realm, categories in info[ 'dir' ][ 'realms' ].items():
            n_realms += 1
            for cat_name, actors in categories.items():
                n_cats += 1
                for actor_uid, endpoint in actors.items():
                    n_actors += 1

        info[ 'n_actors' ] = n_actors
        info[ 'n_realms' ] = n_realms
        info[ 'n_cats' ] = n_cats
        return json.dumps( info )

###############################################################################
# BOILER PLATE
###############################################################################
urls = ( r'/', 'Index',
         r'/info', 'GetClusterInfo' )
web.config.debug = False
app = web.application( urls, globals() )

render = web.template.render( '%s/templates/' % g_current_dir,
                              globals = {} )

if len( sys.argv ) < 2:
    print( "Dashboard takes single argument: cluster config file." )
    sys.exit()
beach = Beach( sys.argv[ -1 ] )
sys.argv.pop()

os.chdir( g_current_dir )
app.run()