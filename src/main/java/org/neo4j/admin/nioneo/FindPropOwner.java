package org.neo4j.admin.nioneo;

import java.rmi.RemoteException;

import org.neo4j.admin.nioneo.store.NodeRecord;
import org.neo4j.admin.nioneo.store.NodeStore;
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
        StringBuffer nodeHits = new StringBuffer();
        for ( int i = 0; i < maxNodeId; i++ )
        {
            NodeRecord record = nodeStore.forceGetRecord( i );
            if ( record.getNextProp() == id )
            {
                nodeHits.append( "node record # " + i + " " );
            }
        }
        if ( nodeHits.length() > 0 )
        {
            hit = nodeHits.toString();
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
