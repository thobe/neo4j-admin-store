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

public class FixRels extends NioneoApp
{

    public String execute( AppCommandParser parser, Session session, Output out )
            throws ShellException
    {
        for ( int i = 0; i < parser.arguments().size(); i++ )
        {
            String arg = parser.arguments().get( i );
            int id = Integer.parseInt( arg );
            RelationshipStore relStore = getServer().getRelStore();
            RelationshipRecord relRecord = relStore.forceGetRecord( id );
            if ( relRecord.inUse() )
            {
                RelationshipRecord prevRecord = relStore.forceGetRecord( relRecord.getFirstPrevRel() );
                if ( prevRecord.getFirstNode() == relRecord.getFirstNode() && 
                    prevRecord.getFirstNextRel() == Record.NO_NEXT_RELATIONSHIP.intValue() )
                {
                    if ( getLastRel( prevRecord.getFirstNode()) == prevRecord.getId() )
                    {
                        prevRecord.setFirstNextRel( relRecord.getId() );
                        relStore.forceUpdateRecord( prevRecord );
                    }
                }
            }
        }
        try
        {
            out.println( "Done" );
        }
        catch ( RemoteException e )
        {
            throw new ShellException( e );
        }
        return null;
    }

    private int getLastRel( int nodeId )
    {
        NodeStore nodeStore = getServer().getNodeStore();
        NodeRecord nodeRecord = nodeStore.getRecord( nodeId );
        RelationshipStore relStore = getServer().getRelStore();
        int nextRelId = nodeRecord.getNextRel();
        int prevRelId = -1;
        while ( nextRelId != Record.NO_PREV_RELATIONSHIP.intValue() )
        {
            RelationshipRecord record = relStore.forceGetRecord( nextRelId );
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
        }
        return prevRelId;
    }
}
