package org.neo4j.admin.nioneo;

import org.neo4j.shell.ShellLobby;

public class Main
{
    public static void main( String args[] ) throws Exception
    {
        NioneoServer server = new NioneoServer( args[0] );
        server.addApp( LsNode.class );
        server.addApp( LsRel.class );
        server.addApp( LsProp.class );
        server.addApp( LsString.class );

        ShellLobby.newClient( server ).grabPrompt();
        server.shutdown();
    }
}
