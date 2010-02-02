package org.neo4j.admin.nioneo;

import java.rmi.RemoteException;
import java.util.Set;
import java.util.HashSet;

import org.neo4j.admin.nioneo.store.NodeRecord;
import org.neo4j.admin.nioneo.store.NodeStore;
import org.neo4j.admin.nioneo.store.Record;
import org.neo4j.admin.nioneo.store.RelationshipRecord;
import org.neo4j.admin.nioneo.store.RelationshipStore;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;

public class CheckAllRelChains extends NioneoApp
{

    public String execute( AppCommandParser parser, Session session, Output out )
            throws ShellException
    {
        int count = 0;
        try
        {
        RelationshipStore relStore = getServer().getRelStore();
        NodeStore nodeStore = getServer().getNodeStore();
        int maxNodeId = (int) nodeStore.getHighId();
        Set<Integer> chainedRels = new HashSet<Integer>();
        for ( int i = 0; i < maxNodeId; i++ )
        {
            NodeRecord nodeRecord = nodeStore.forceGetRecord( i );
            if ( !nodeRecord.inUse() )
            {
                continue;
            }
            int nextRelId = nodeRecord.getNextRel();
            int prevRelId = -1;
            while ( nextRelId != Record.NO_PREV_RELATIONSHIP.intValue() )
            {
                RelationshipRecord record = relStore.forceGetRecord( nextRelId );
                chainedRels.add( record.getId() );
                if ( !record.inUse() && prevRelId != -1 )
                {
                    RelationshipRecord prevRecord = relStore.forceGetRecord( prevRelId );
                    if( prevRecord.getFirstNode() == i )
                    {
                        prevRecord.setFirstNextRel( Record.NO_NEXT_RELATIONSHIP.intValue() );
                    }
                    else if ( prevRecord.getSecondNode() == i )
                    {
                        prevRecord.setSecondNextRel( Record.NO_NEXT_RELATIONSHIP.intValue() );
                    }
                    count++;
                    relStore.forceUpdateRecord( prevRecord );
                    break;
                }
                if ( record.getFirstNode() == i )
                {
                    if ( record.getFirstPrevRel() != prevRelId )
                    {
                        record.setFirstPrevRel( prevRelId );
                        relStore.forceUpdateRecord( record );
                        count++;
                    }
                    prevRelId = nextRelId;
                    nextRelId = record.getFirstNextRel();
                }
                else if ( record.getSecondNode() == i )
                {
                    if ( record.getSecondPrevRel() != prevRelId )
                    {
                        record.setSecondPrevRel( prevRelId );
                        relStore.forceUpdateRecord( record );
                        count++;
                    }
                    prevRelId = nextRelId;
                    nextRelId = record.getSecondNextRel();
                }
                else
                {
                    System.out.println( "Error going from rel " + prevRelId + " to " + nextRelId );
                    nextRelId = Record.NO_PREV_RELATIONSHIP.intValue();
                }
            }
        }
        }catch ( Throwable t )
        {
            t.printStackTrace();
        }
        try
        {
            out.println( count + " rel chain links modified" );
        }
        catch ( RemoteException e )
        {
            throw new ShellException( e );
        }
        return null;
    }
}
