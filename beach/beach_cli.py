#!/usr/bin/env python
'''A Command Line Interface to manage the beach cluster. Instantiate like:
    python -m beach.beach_cli -h
'''

import cmd
import readline
import argparse
import sys
import inspect
import shlex
import uuid
import urllib
import json
import traceback
import os
from beach.beach_api import Beach
from beach.utils import *

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

    def __init__( self, configFile ):
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
        '''Retrieve a specific user's profile by UID.'''
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

        if isMessageSuccess( resp ) and 'realms' in resp:
            wanted = resp[ 'realms' ].get( self.realm, {} )

            if category is not None:
                wanted = wanted.get( category, {} )

        self.printOut( wanted )

    @report_errors
    def do_flush( self, s ):
        '''Retrieve a specific user's profile by UID.'''
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
        '''Retrieve a specific user's profile by UID.'''
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
                             help = 'only show the directory for a specific category.' )
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
        arguments = self.parse( parser, s )

        if arguments is None:
            return

        resp = self.beach.addActor( arguments.name, arguments.category, arguments.strategy, arguments.strat_hint )

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

        arguments = self.parse( parser, s )

        if arguments is None:
            return
        if arguments.id is None and arguments.cat is None:
            argparse.error( 'Must specify one of -i or -c.' )

        resp = self.beach.stopActors( withId = arguments.id, withCategory = arguments.cat )

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

if __name__ == '__main__':
    if 2 != len( sys.argv ):
        print( "Usage: beach_cli.py pathToBeachConfigFile" )
    else:
        app = BeachShell( sys.argv[ 1 ] )
        app.cmdloop()