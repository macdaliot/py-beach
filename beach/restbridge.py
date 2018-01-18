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
import msgpack
import tempfile
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

def msgpackApi( f ):
    ''' Decorator to basic exception handling on function. '''
    @wraps( f )
    def wrapped( *args, **kwargs ):
        web.header( 'Content-Type', 'application/msgpack' )
        r = f( *args, **kwargs )
        try:
            return msgpack.packb( r )
        except:
            return msgpack.packb( { 'error' : str( r ) } )
    return wrapped

def jsonData( data ):
    web.header( 'Content-Type', 'application/json' )
    return json.dumps( data )

def msgpackData( data ):
    web.header( 'Content-Type', 'application/msgpack' )
    return msgpack.packb( data )

###############################################################################
# PAGES
###############################################################################
class Bridge:
    def GET( self, category ):
        if not ENABLE_GET:
            raise web.HTTPError( '405 Method Not Allowed' )
        return self.action( category )

    def POST( self, category ):
        return self.action( category )

    def action( self, category ):
        data = {}

        params = web.input( _timeout = None, 
                            _ident = None, 
                            _key = None, 
                            _action = None, 
                            _secret = None, 
                            _format = 'json', 
                            _json_data = None,
                            _from_all = None )

        ident = params._ident
        timeout = params._timeout
        key = params._key
        action = params._action
        secret = params._secret
        fromAll = params._from_all

        if action is None:
            action = web.ctx.env.get( 'HTTP_BEACH_ACTION', None )
            ident = web.ctx.env.get( 'HTTP_BEACH_IDENT', None )
            timeout = web.ctx.env.get( 'HTTP_BEACH_TIMEOUT', None )
            key = web.ctx.env.get( 'HTTP_BEACH_KEY', None )
            secret = web.ctx.env.get( 'HTTP_BEACH_SECRET', None )
            fromAll = web.ctx.env.get( 'HTTP_BEACH_FROM_ALL', None )

        if SECRET_TOKEN is not None and secret != SECRET_TOKEN:
            raise web.HTTPError( '401 Unauthorized' )

        if fromAll is not None:
            fromAll = ( fromAll == 'true' )

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

        if params._json_data is not None:
            req.update( json.loads( params._json_data ) )

        cacheKey = '%s-%s' % ( category, ident )

        if cacheKey in handle_cache:
            handle = handle_cache[ cacheKey ]
        else:
            print( "New handle: %s / %s / %s / %s" % ( category, timeout, ident, fromAll ) )
            handle_cache[ cacheKey ] = beach.getActorHandle( category, ident = ident )
            handle = handle_cache[ cacheKey ]

        #print( "Requesting: %s - %s" % ( action, req ) )
        
        if fromAll:
            respFuture = handle.requestFromAll( action, data = req, timeout = timeout )

            data = []
            while respFuture.waitForResults( timeout = timeout ):
                data += [ x.data for x in respFuture.getNewResults() if x.isSuccess ]
                if respFuture.isFinished():
                    break
        else:
            resp = handle.request( action, data = req, timeout = timeout, key = key )

            if resp.isTimedOut:
                raise web.HTTPError( '500 Internal Server Error: Request timed out' )

            if not resp.isSuccess:
                raise web.HTTPError( '500 Internal Server Error: Failed - %s' % resp )

            data = resp.data

        #print( "Response: %s" % ( json.dumps( data, indent = 2 ), ) )

        if 'json' == params._format:
            return jsonData( data )
        elif 'msgpack' == params._format:
            return msgpackData( data )
        else:
            raise web.HTTPError( '500 Internal Server Error: Unknown Format' )


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
    parser.add_argument( '--ssl-cert',
                         required = False,
                         dest = 'sslCert',
                         default = os.path.abspath( 'rest.beach.crt' ),
                         help = 'path to the SSL cert to use' )
    parser.add_argument( '--ssl-key',
                         required = False,
                         dest = 'sslKey',
                         default = os.path.abspath( 'rest.beach.key' ),
                         help = 'path to the SSL key to use' )
    parser.add_argument( '--ssl-self-signed',
                         required = False,
                         dest = 'sslSelfSigned',
                         action = 'store_true',
                         default = False,
                         help = 'if set a self-signed certificate will be generated and used' )
    args = parser.parse_args()

    SECRET_TOKEN = args.secret
    ENABLE_GET = args.withGet

    if args.sslSelfSigned:
        if not os.path.isfile( args.sslCert ) and not os.path.isfile( args.sslKey ):
            print( "Generating self-signed certs." )
        if 0 != os.system( 'openssl req -x509 -days 36500 -newkey rsa:4096 -keyout %s -out %s -nodes -sha256 -subj "/C=US/ST=CA/L=Mountain View/O=refractionPOINT/CN=restbridge.beach" > /dev/null 2>&1' % ( args.sslKey, args.sslCert ) ):
            print( "Failed to generate self-signed certificate." )

    if os.path.isfile( args.sslCert ) and os.path.isfile( args.sslKey ):
        print( "Using SSL cert/key: %s and %s" % ( args.sslCert, args.sslKey ) )
        from web.wsgiserver import CherryPyWSGIServer
        CherryPyWSGIServer.ssl_certificate = args.sslCert
        CherryPyWSGIServer.ssl_private_key = args.sslKey
    else:
        print( "No SSL cert/key at %s and %s so using normal HTTP." % ( args.sslCert, args.sslKey ) )

    urls = ( r'/(.*)', 'Bridge', )
    web.config.debug = False
    app = web.application( urls, globals() )
    beach = Beach( args.configFile, realm = args.realm )
    handle_cache = {}

    os.chdir( g_current_dir )
    app.run()