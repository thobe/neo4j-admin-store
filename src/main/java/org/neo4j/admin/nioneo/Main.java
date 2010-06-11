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
        server.addApp( ListRels.class );
        server.addApp( RelDel.class );
        server.addApp( CheckRels.class );
        server.addApp( CheckAllRelChains.class );
        server.addApp( CheckRelChain.class );
        server.addApp( FindPropOwner.class );
        server.addApp( FindStartRecordsForNode.class );
        server.addApp( FindStartRelForRelNode.class );
        server.addApp( DumpPrevChainForRelNode.class );
        server.addApp( FixRels.class );
        server.addApp( FixNodeProps.class );
        server.addApp( FixNodeProps2.class );
        server.addApp( Load.class );
        server.addApp( Ls.class );
        server.addApp( Set.class );
        server.addApp( Store.class );
        server.addApp( Toggle.class );
        ShellLobby.newClient( server ).grabPrompt();
        server.shutdown();
    }
}
