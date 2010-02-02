package org.neo4j.admin.nioneo;

import java.rmi.RemoteException;

import org.neo4j.admin.nioneo.store.NodeRecord;
import org.neo4j.admin.nioneo.store.NodeStore;
import org.neo4j.admin.nioneo.store.Record;
import org.neo4j.admin.nioneo.store.RelationshipRecord;
import org.neo4j.admin.nioneo.store.RelationshipStore;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;

public class ListRels extends NioneoApp
{

    public String execute( AppCommandParser parser, Session session, Output out )
            throws ShellException
    {
        String arg = parser.arguments().get( 0 );
        int nodeId = Integer.parseInt( arg );
        NodeStore nodeStore = getServer().getNodeStore();
        NodeRecord nodeRecord = nodeStore.getRecord( nodeId );
        RelationshipStore relStore = getServer().getRelStore();
        int nextRelId = nodeRecord.getNextRel();
        int prevRelId = -1;
        StringBuffer hits = new StringBuffer();
        while ( nextRelId != Record.NO_PREV_RELATIONSHIP.intValue() )
        {
            RelationshipRecord record = relStore.forceGetRecord( nextRelId );
            if ( !record.inUse() )
            {
                hits.append( "Error: " + nextRelId + " not in use." );
            }
            hits.append( nextRelId + " " );
            if ( record.getFirstNode() == nodeId )
            {
                prevRelId = nextRelId;
                nextRelId = record.getFirstNextRel();
            }
            else if ( record.getSecondNode() == nodeId )
            {
                prevRelId = nextRelId;
                nextRelId = record.getSecondNextRel();
            }
            else
            {
                System.out.println( "Error going from rel " + prevRelId + " to " + nextRelId );
                nextRelId = Record.NO_PREV_RELATIONSHIP.intValue();
            }
        }
        try
        {
            out.println( hits.toString() );
        }
        catch ( RemoteException e )
        {
            throw new ShellException( e );
        }
        return null;
    }
}
