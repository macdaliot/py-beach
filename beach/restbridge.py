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
SECRET_TOKEN = None
ENABLE_GET = False

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
        if not ENABLE_GET:
            raise web.HTTPError( '405 Method Not Allowed' )
        return self.action( category )

    @jsonApi
    def POST( self, category ):
        return self.action( category )

    def action( self, category ):
        data = {}

        params = web.input( _timeout = None, _ident = None, _key = None, _action = None, _secret = None )

        ident = params._ident
        timeout = params._timeout
        key = params._key
        action = params._action
        secret = params._secret

        if action is None:
            action = web.ctx.env.get( 'HTTP_BEACH_ACTION', None )
            ident = web.ctx.env.get( 'HTTP_BEACH_IDENT', None )
            timeout = web.ctx.env.get( 'HTTP_BEACH_TIMEOUT', None )
            key = web.ctx.env.get( 'HTTP_BEACH_KEY', None )
            secret = web.ctx.env.get( 'HTTP_BEACH_SECRET', None )

        if SECRET_TOKEN is not None and secret != SECRET_TOKEN:
            raise web.HTTPError( '401 Unauthorized' )

        if action is None:
            # We support api.ai
            try:
                data = json.loads( web.data() )
                if data:
                    action = data.get( 'result', {} ).get( 'action', None )
                    if action is not None:
                        params = data
            except:
                action = None

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

        #print( "Requesting: %s - %s" % ( action, req ) )
        
        resp = handle.request( action, data = req, timeout = timeout, key = key )

        if resp.isTimedOut:
            raise web.HTTPError( '500 Internal Server Error: Request timed out' )

        if not resp.isSuccess:
            raise web.HTTPError( '500 Internal Server Error: Failed - %s' % resp.error )

        data = resp.data

        #print( "Response: %s" % ( json.dumps( data, indent = 2 ), ) )

        return data


###############################################################################
# BOILER PLATE
###############################################################################
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser( prog = 'RestBridge' )
    parser.add_argument( 'port',
                         type = int,
                         help = 'the port to listen on' )
    parser.add_argument( 'configFile',
                         type = str,
                         help = 'the main config file defining the beach cluster' )
    parser.add_argument( 'realm',
                         type = str,
                         help = 'the realm to give access to' )
    parser.add_argument( '-s', '--secret',
                         type = str,
                         required = False,
                         dest = 'secret',
                         help = 'an optional secret token that must be provided as the _secret header' )
    parser.add_argument( '-g', '--withget',
                         required = False,
                         dest = 'withGet',
                         action = 'store_true',
                         default =  False,
                         help = 'if set HTTP GET is also accepted' )
    args = parser.parse_args()

    SECRET_TOKEN = args.secret
    ENABLE_GET = args.withGet

    urls = ( r'/(.*)', 'Bridge', )
    web.config.debug = False
    app = web.application( urls, globals() )
    beach = Beach( args.configFile, realm = args.realm )
    handle_cache = {}

    os.chdir( g_current_dir )
    app.run()