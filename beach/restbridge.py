# Copyright (C) 2015  refractionPOINT
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.

import os
import sys

g_current_dir = os.path.dirname( os.path.abspath( __file__ ) )

from beach.beach_api import Beach

import traceback
import web
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
def sanitizeJson( o, summarized = False ):
    if type( o ) is dict:
        for k, v in o.iteritems():
            o[ k ] = sanitizeJson( v, summarized = summarized )
    elif type( o ) is list or type( o ) is tuple:
        o = [ sanitizeJson( x, summarized = summarized ) for x in o ]
    else:
        try:
            json.dumps( o )
        except:
            o = base64.b64encode( o )
        if summarized is not False and len( str( o ) ) > summarized:
            o = str( o[ : summarized ] ) + '...'

    return o


###############################################################################
# PAGE DECORATORS
###############################################################################
def jsonApi( f ):
    ''' Decorator to basic exception handling on function. '''
    @wraps( f )
    def wrapped( *args, **kwargs ):
        web.header( 'Content-Type', 'application/json' )
        r = f( *args, **kwargs )
        try:
            return json.dumps( r )
        except:
            return json.dumps( { 'error' : str( r ) } )
    return wrapped


###############################################################################
# PAGES
###############################################################################
class Bridge:
    @jsonApi
    def GET( self, category ):
        data = {}

        params = web.input( _timeout = None, _ident = None, _key = None, _action = None )

        ident = params._ident
        timeout = params._timeout
        key = params._key
        action = params._action

        if action is None:
            raise web.HTTPError( '400 Bad Request: _action parameter required for requestType' )

        if timeout is not None:
            timeout = float( timeout )

        req = {}
        for k, v in params.iteritems():
            if not k.startswith( '_' ):
                req[ k ] = v

        cacheKey = '%s-%s' % ( category, ident )

        if cacheKey in handle_cache:
            handle = handle_cache[ cacheKey ]
        else:
            print( "New handle: %s / %s / %s" % ( category, timeout, ident ) )
            handle_cache[ cacheKey ] = beach.getActorHandle( category, ident = ident )
            handle = handle_cache[ cacheKey ]

        print( "Requesting: %s - %s" % ( action, req ) )
        resp = handle.request( action, data = req, timeout = timeout )

        if resp.isTimedOut:
            raise web.HTTPError( '500 Internal Server Error: Request timed out' )

        if not resp.isSuccess:
            raise web.HTTPError( '500 Internal Server Error: Failed - %s' % resp.error )

        data = resp.data

        return data


###############################################################################
# BOILER PLATE
###############################################################################
urls = ( r'/(.*)', 'Bridge', )
web.config.debug = False
app = web.application( urls, globals() )

if len( sys.argv ) < 2:
    print( "python -m beach.restbridge [port] beachConfigFilePath realm" )
    sys.exit()
realm = sys.argv[ -1 ]
sys.argv.pop()
beach = Beach( sys.argv[ -1 ], realm = realm )
sys.argv.pop()
handle_cache = {}

os.chdir( g_current_dir )
app.run()