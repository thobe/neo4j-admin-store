package org.neo4j.admin.nioneo;

import java.rmi.RemoteException;

import org.neo4j.admin.nioneo.store.NodeRecord;
import org.neo4j.admin.nioneo.store.NodeStore;
import org.neo4j.admin.nioneo.store.RelationshipRecord;
import org.neo4j.admin.nioneo.store.RelationshipStore;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;

public class FindPropOwner extends NioneoApp
{

    public String execute( AppCommandParser parser, Session session, Output out )
            throws ShellException
    {
        String arg = parser.arguments().get( 0 );
        int id = Integer.parseInt( arg );
        NodeStore nodeStore = getServer().getNodeStore();
        int maxNodeId = (int) nodeStore.getHighId();
        String hit = "Not found";
        StringBuffer hits = new StringBuffer();
        for ( int i = 0; i < maxNodeId; i++ )
        {
            NodeRecord record = nodeStore.forceGetRecord( i );
            if ( record.getNextProp() == id )
            {
                hits.append( "node record # " + i + " " );
            }
        }
        RelationshipStore relStore = getServer().getRelStore();
        int maxRelId = (int) relStore.getHighId();
        for ( int i = 0; i < maxNodeId; i++ )
        {
            RelationshipRecord record = relStore.forceGetRecord( i );
            if ( record.getNextProp() == id )
            {
                hits.append( "rel record # " + i + " " );
            }
        }
        if ( hits.length() > 0 )
        {
            hit = hits.toString();
        }
        try
        {
            out.println( hit );
        }
        catch ( RemoteException e )
        {
            throw new ShellException( e );
        }
        return null;
    }

}
