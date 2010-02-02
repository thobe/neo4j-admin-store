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

public class RelDel extends NioneoApp
{

    public String execute( AppCommandParser parser, Session session, Output out )
            throws ShellException
    {
        String arg = parser.arguments().get( 0 );
        int relId = Integer.parseInt( arg );
        RelationshipStore relStore = getServer().getRelStore();
        RelationshipRecord record = relStore.forceGetRecord( relId );
        try
        {
            if ( record.inUse() )
            {
                relDelete( record );
                out.println( "Deleted " + relId );
            }
            else
            {
                out.println( "Rel " + relId + " already deleted" );
            }
        }
        catch ( RemoteException e )
        {
            throw new ShellException( e );
        }
        return null;
    }

    void relDelete( RelationshipRecord rel )
    {
        RelationshipStore relStore = getServer().getRelStore();
        if ( rel.getFirstPrevRel() != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            RelationshipRecord prevRel = relStore.forceGetRecord( rel.getFirstPrevRel() );
            if ( prevRel.inUse() )
            {
                if ( prevRel.getFirstNode() == rel.getFirstNode() && 
                    prevRel.getFirstNextRel() == rel.getId() )
                {
                    prevRel.setFirstNextRel( rel.getFirstNextRel() );
                }
                else if ( prevRel.getSecondNode() == rel.getFirstNode() && 
                    prevRel.getSecondNextRel() == rel.getId() )
                {
                    prevRel.setSecondNextRel( rel.getFirstNextRel() );
                }
                relStore.forceUpdateRecord( prevRel );
            }
        }
        // update first node next
        if ( rel.getFirstNextRel() != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            RelationshipRecord nextRel = relStore.forceGetRecord( rel.getFirstNextRel() );
            if ( nextRel.inUse() )
            {
                if ( nextRel.getFirstNode() == rel.getFirstNode() && 
                    nextRel.getFirstPrevRel() == rel.getId() )
                {
                    nextRel.setFirstPrevRel( rel.getFirstPrevRel() );
                }
                else if ( nextRel.getSecondNode() == rel.getFirstNode() && 
                    nextRel.getSecondPrevRel() == rel.getId() )
                {
                    nextRel.setSecondPrevRel( rel.getFirstPrevRel() );
                }
                relStore.forceUpdateRecord( nextRel );
            }
        }
        // update second node prev
        if ( rel.getSecondPrevRel() != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            RelationshipRecord prevRel = relStore.forceGetRecord( rel.getSecondPrevRel() );
            if ( prevRel.inUse() )
            {
                if ( prevRel.getFirstNode() == rel.getSecondNode() && 
                    prevRel.getFirstNextRel() == rel.getId() )
                {
                    prevRel.setFirstNextRel( rel.getSecondNextRel() );
                }
                else if ( prevRel.getSecondNode() == rel.getSecondNode() && 
                    prevRel.getSecondNextRel() == rel.getId() )
                {
                    prevRel.setSecondNextRel( rel.getSecondNextRel() );
                }
                relStore.forceUpdateRecord( prevRel );
            }
        }
        // update second node next
        if ( rel.getSecondNextRel() != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            RelationshipRecord nextRel = relStore.forceGetRecord( rel.getSecondNextRel() );
            if ( nextRel.inUse() )
            {
                if ( nextRel.getFirstNode() == rel.getSecondNode() && 
                    nextRel.getFirstPrevRel() == rel.getId() )
                {
                    nextRel.setFirstPrevRel( rel.getSecondPrevRel() );
                }
                else if ( nextRel.getSecondNode() == rel.getSecondNode() && 
                    nextRel.getSecondPrevRel() == rel.getId() )
                {
                    nextRel.setSecondPrevRel( rel.getSecondPrevRel() );
                }
                relStore.forceUpdateRecord( nextRel );
            }
        }

        NodeStore nodeStore = getServer().getNodeStore();
        if ( rel.getFirstPrevRel() == Record.NO_PREV_RELATIONSHIP.intValue() )
        {
            NodeRecord firstNode = nodeStore.forceGetRecord( rel.getFirstNode() );
            if ( firstNode.inUse() && firstNode.getNextRel() == rel.getId() )
            {
                firstNode.setNextRel( rel.getFirstNextRel() );
                nodeStore.forceUpdateRecord( firstNode );
            }
        }
        if ( rel.getSecondPrevRel() == Record.NO_PREV_RELATIONSHIP.intValue() )
        {
            NodeRecord secondNode = nodeStore.forceGetRecord( rel.getSecondNode() );
            if ( secondNode.inUse() && secondNode.getNextRel() == rel.getId() )
            {
                secondNode.setNextRel( rel.getSecondNextRel() );
                nodeStore.forceUpdateRecord( secondNode );
            }
        }
        rel.setInUse( false );
        relStore.forceUpdateRecord( rel );
    }
}
