#!/usr/bin/env python

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

'''A Command Line Interface to manage the beach cluster. Instantiate like:
    python -m beach.beach_cli -h
'''

import sys
import cmd
import argparse
import inspect
import shlex
import yaml
import json
import traceback
from beach.beach_api import Beach
try:
    import M2Crypto
except:
    print( "Beach crypto facilities disable due to failure to load M2Crypto." )

def report_errors( func ):
    def silenceit( *args, **kwargs ):
        try:
            return func( *args,**kwargs )
        except:
            print( traceback.format_exc() )
            return None
    return( silenceit )

class BeachShell ( cmd.Cmd ):
    intro = 'Welcome to Beach shell.   Type help or ? to list commands.\n'
    prompt = '(beach) '

    def __init__( self, configFile, realm = None ):
        cmd.Cmd.__init__( self )
        self.realm = 'global'
        self.updatePrompt()
        self.beach = Beach( configFile )

    def updatePrompt( self ):
        if self.realm is not None:
            self.prompt = '(beach/%s) ' % self.realm
        else:
            self.prompt = '(beach/global) '

    def parse( self, parser, line ):
        try:
            return parser.parse_args( shlex.split( line ) )
        except SystemExit:
            return None

    def do_exit( self, s ):
        self.beach.close()
        return True

    def do_quit( self, s ):
        self.beach.close()
        return True

    def emptyline( self ):
        pass

    def printOut( self, data ):
        print( json.dumps( data, indent = 4 ) )

    @report_errors
    def do_gen_key( self, s ):
        '''Generate a key that can be used as a beach private key.'''
        parser = argparse.ArgumentParser( prog = inspect.stack()[0][3][ 3 : ] )

        parser.add_argument( 'out',
                             type = str,
                             help = 'the path where to store the key.' )
        arguments = self.parse( parser, s )

        if arguments is None:
            return

        with open( arguments.out, 'w' ) as f:
            f.write( M2Crypto.Rand.rand_bytes( 0x20 ) )

        self.printOut( 'New private key written to %s.' % arguments.out )

    @report_errors
    def do_realm( self, s ):
        '''Login as a specific user.'''
        parser = argparse.ArgumentParser( prog = inspect.stack()[0][3][ 3 : ] )

        parser.add_argument( 'realm',
                             type = str,
                             default  = 'global',
                             help = 'switch context to a specific realm.' )
        arguments = self.parse( parser, s )

        if arguments is None:
            return

        self.realm = arguments.realm

        if self.realm is None or self.realm.strip() == '':
            self.ream = 'global'

        self.updatePrompt()
        self.beach.setRealm( self.realm )

    @report_errors
    def do_get_dir( self, s ):
        '''Retrieve the directory of all Actors.'''
        parser = argparse.ArgumentParser( prog = inspect.stack()[0][3][ 3 : ] )
        parser.add_argument( '-c', '--category',
                             type = str,
                             dest = 'category',
                             default = None,
                             help = 'only show the directory for a specific category.' )
        arguments = self.parse( parser, s )

        if arguments is None:
            return

        category = arguments.category

        resp = self.beach.getDirectory()

        wanted = False

        if resp is not False and 'realms' in resp:
            wanted = resp[ 'realms' ].get( self.realm, {} )

            if category is not None:
                wanted = wanted.get( category, {} )

        self.printOut( wanted )

    @report_errors
    def do_flush( self, s ):
        '''Remove all Actors from all nodes in the cluster.'''
        parser = argparse.ArgumentParser( prog = inspect.stack()[0][3][ 3 : ] )

        parser.add_argument( '--confirm',
                             action = 'store_true',
                             help = 'This command flushes ALL ACTORS from the cluster REGARDLESS of the realm. '
                                    'Add this flag to confirm you understand this.' )
        arguments = self.parse( parser, s )

        if arguments is None:
            return

        resp = 'Please confirm ( see command help )'
        if arguments.confirm:
            resp = self.beach.flush()

        self.printOut( resp )

    @report_errors
    def do_add_actor( self, s ):
        '''Add a new Actor to the cluster.'''
        parser = argparse.ArgumentParser( prog = inspect.stack()[0][3][ 3 : ] )
        parser.add_argument( '-n', '--name',
                             type = str,
                             dest = 'name',
                             required = True,
                             help = 'the name of the actor to spawn.' )
        parser.add_argument( '-c', '--category',
                             type = str,
                             dest = 'category',
                             required = True,
                             nargs = '+',
                             help = 'category or categories to add the Actor to.' )
        parser.add_argument( '-s', '--strategy',
                             type = str,
                             dest = 'strategy',
                             default = None,
                             help = 'the strategy to use to spawn the actor in the beach.' )
        parser.add_argument( '-sh', '--hint',
                             type = str,
                             dest = 'strat_hint',
                             default = None,
                             help = 'hint used as part of some strategies.' )
        parser.add_argument( '-p', '--params',
                             type = json.loads,
                             dest = 'params',
                             default = None,
                             help = 'parameters to provide to the Actor, as a JSON string.' )
        parser.add_argument( '-i', '--isisolated',
                             dest = 'isIsolated',
                             default = False,
                             action = 'store_true',
                             help = 'if the Actor should be started in isolation mode (standalone process).' )
        parser.add_argument( '-id', '--ident',
                             type = str,
                             dest = 'ident',
                             default = None,
                             help = 'identifier secret token used for Actor trust model.' )
        parser.add_argument( '-t', '--trusted',
                             type = str,
                             dest = 'trusted',
                             default = [],
                             action = 'append',
                             help = 'identifier token trusted by the Actor trust model.' )
        parser.add_argument( '-ll', '--log-level',
                             type = str,
                             dest = 'loglevel',
                             default = None,
                             help = 'custom logging level for actor.' )
        parser.add_argument( '-ld', '--log-dest',
                             type = str,
                             dest = 'logdest',
                             default = None,
                             help = 'custom logging destination for actor.' )
        parser.add_argument( '-o', '--concurrent',
                             type = int,
                             dest = 'n_concurrent',
                             required = False,
                             default = 1,
                             help = 'the number of concurrent requests handled by the actor.' )
        parser.add_argument( '-d', '--isdrainable',
                             dest = 'isDrainable',
                             default = False,
                             action = 'store_true',
                             help = 'if the Actor can be requested to drain gracefully.' )
        arguments = self.parse( parser, s )

        if arguments is None:
            return

        resp = self.beach.addActor( arguments.name,
                                    arguments.category,
                                    strategy = arguments.strategy,
                                    strategy_hint = arguments.strat_hint,
                                    parameters = arguments.params,
                                    isIsolated = arguments.isIsolated,
                                    secretIdent = arguments.ident,
                                    trustedIdents = arguments.trusted,
                                    n_concurrent = arguments.n_concurrent,
                                    is_drainable = arguments.isDrainable,
                                    log_level = arguments.log_level,
                                    log_dest = arguments.log_dest )

        self.printOut( resp )

    @report_errors
    def do_stop_actor( self, s ):
        '''Stop a specific set of actors.'''
        parser = argparse.ArgumentParser( prog = inspect.stack()[0][3][ 3 : ] )
        parser.add_argument( '-i', '--id',
                             type = str,
                             dest = 'id',
                             required = False,
                             nargs = '+',
                             help = 'the IDs of actors to stop.' )
        parser.add_argument( '-c', '--category',
                             type = str,
                             dest = 'cat',
                             required = False,
                             nargs = '+',
                             help = 'the categories of actors to stop.' )
        parser.add_argument( '-d', '--delay',
                             type = int,
                             dest = 'delay',
                             required = False,
                             default = None,
                             help = 'the number of seconds between stopping each actor.' )

        arguments = self.parse( parser, s )

        if arguments is None:
            return
        if arguments.id is None and arguments.cat is None:
            argparse.error( 'Must specify one of -i or -c.' )

        resp = self.beach.stopActors( withId = arguments.id, withCategory = arguments.cat, delay = arguments.delay )

        self.printOut( resp )

    @report_errors
    def do_get_cluster_health( self, s ):
        '''Retrieve the health information of all nodes of the cluster.'''
        parser = argparse.ArgumentParser( prog = inspect.stack()[0][3][ 3 : ] )
        arguments = self.parse( parser, s )

        if arguments is None:
            return

        resp = self.beach.getClusterHealth()

        self.printOut( resp )

    @report_errors
    def do_get_load_info( self, s ):
        '''Retrieve the number of free handlers per actor.'''
        parser = argparse.ArgumentParser( prog = inspect.stack()[0][3][ 3 : ] )
        arguments = self.parse( parser, s )

        if arguments is None:
            return

        resp = self.beach.getLoadInfo()

        self.printOut( resp )

    @report_errors
    def do_get_mtd( self, s ):
        '''Retrieve metadata from all nodes.'''
        parser = argparse.ArgumentParser( prog = inspect.stack()[0][3][ 3 : ] )

        resp = self.beach.getAllNodeMetadata()

        self.printOut( resp )

    @report_errors
    def do_remove_from_category( self, s ):
        '''Remove an Actor from a category.'''
        parser = argparse.ArgumentParser( prog = inspect.stack()[0][3][ 3 : ] )
        parser.add_argument( '-i', '--id',
                             type = str,
                             dest = 'id',
                             required = True,
                             help = 'the ID of the actor to add to the category.' )
        parser.add_argument( '-c', '--category',
                             type = str,
                             dest = 'category',
                             required = True,
                             help = 'category to add the actor to.' )
        arguments = self.parse( parser, s )

        if arguments is None:
            return

        resp = self.beach.removeFromCategory( arguments.id,
                                         arguments.category )

        self.printOut( resp )

    @report_errors
    def do_add_to_category( self, s ):
        '''Add an Actor to a category.'''
        parser = argparse.ArgumentParser( prog = inspect.stack()[0][3][ 3 : ] )
        parser.add_argument( '-i', '--id',
                             type = str,
                             dest = 'id',
                             required = True,
                             help = 'the ID of the actor to add to the category.' )
        parser.add_argument( '-c', '--category',
                             type = str,
                             dest = 'category',
                             required = True,
                             help = 'category to add the actor to.' )
        arguments = self.parse( parser, s )

        if arguments is None:
            return

        resp = self.beach.addToCategory( arguments.id,
                                         arguments.category )

        self.printOut( resp )

if __name__ == '__main__':
    conf = None

    parser = argparse.ArgumentParser( description = 'CLI for a Beach cluster.' )

    parser.add_argument( 'config',
                         type = str,
                         default = None,
                         help = 'cluster config file' )

    parser.add_argument( '--req-realm',
                         type = str,
                         required = False,
                         default = None,
                         dest = 'req_realm',
                         help = 'the realm to issue the request to' )

    parser.add_argument( '--req-cat',
                         type = str,
                         required = False,
                         default = None,
                         dest = 'req_cat',
                         help = 'category to issue a request against' )

    parser.add_argument( '--req-cmd',
                         type = str,
                         required = False,
                         default = None,
                         dest = 'req_cmd',
                         help = 'command to issue with request' )

    parser.add_argument( '--req-data',
                         type = json.loads,
                         required = False,
                         default = '{}',
                         dest = 'req_data',
                         help = 'data, as JSON to include in the request' )

    parser.add_argument( '--req-ident',
                         type = str,
                         required = False,
                         default = None,
                         dest = 'req_ident',
                         help = 'identity to assume while issuing the request' )

    args = parser.parse_args()
    if args.config is None:
        with open( '~/.beach', 'r' ) as f:
            userConf = yaml.parse( f.read() )
            if 'config' in userConf:
                conf = userConf[ 'config' ]
    else:
        conf = args.config

    if conf is None:
        parser.error( 'no config specified and ~/.beach is not a valid config.' )
    else:
        if args.req_realm is None and args.req_cat is None and args.req_cmd is None and args.req_ident is None:
            app = BeachShell( conf )
            app.cmdloop()
        elif args.req_realm is None or args.req_cat is None or args.req_cmd is None or args.req_ident is None:
            parser.error( '--req-* components missing to execute a request.' )
        else:
            beach = Beach( conf, realm = args.req_realm )
            h = beach.getActorHandle( args.req_cat, ident = args.req_ident )
            resp = h.request( args.req_cmd, data = args.req_data )
            h.close()
            beach.close()
            if not resp.isSuccess:
                print( resp )
                sys.exit( 1 )
            else:
                print( json.dumps( resp.data, indent = 4 ) )
                sys.exit( 0 )