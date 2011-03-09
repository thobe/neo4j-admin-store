/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.admin.nioneo;

import java.rmi.RemoteException;

import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeStoreAccess;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipStoreAccess;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;

public class CheckRelChain extends NioneoApp
{

    public String execute( AppCommandParser parser, Session session, Output out )
            throws ShellException
    {
        String arg = parser.arguments().get( 0 );
        int nodeId = Integer.parseInt( arg );
        NodeStoreAccess nodeStore = getServer().getNodeStore();
        NodeRecord nodeRecord = nodeStore.getRecord( nodeId );
        RelationshipStoreAccess relStore = getServer().getRelStore();
        String hit = "No rels found";
        long nextRelId = nodeRecord.getNextRel();
        long prevRelId = -1;
        boolean error = false;
        StringBuffer hits = new StringBuffer();
        while ( nextRelId != Record.NO_PREV_RELATIONSHIP.intValue() )
        {
            hits.append( nextRelId + " " );
            RelationshipRecord record = relStore.getRecord( nextRelId );
            if ( record.getFirstNode() == nodeId )
            {
                NodeRecord otherNode = nodeStore.forceGetRecord( record.getSecondNode() );
                if ( !otherNode.inUse() )
                {
                    relDelete( record );
                    hits.append( "<-deleted "  );
                }
                prevRelId = nextRelId;
                nextRelId = record.getFirstNextRel();
            }
            else if ( record.getSecondNode() == nodeId )
            {
                NodeRecord otherNode = nodeStore.forceGetRecord( record.getFirstNode() );
                if ( !otherNode.inUse() )
                {
                    relDelete( record );
                    hits.append( "<-deleted "  );
                }
                prevRelId = nextRelId;
                nextRelId = record.getSecondNextRel();
            }
            else
            {
                hit = "Error going from rel " + prevRelId + " to " + nextRelId;
                nextRelId = Record.NO_PREV_RELATIONSHIP.intValue();
                error = true;
            }
        }
        if ( !error && hits.length() > 0 )
        {
            hit = hits.toString();
        }
        try
        {
            out.println( hit );
        }
        catch ( RemoteException e )
        {
            throw ShellException.wrapCause( e );
        }
        return null;
    }

    void relDelete( RelationshipRecord rel )
    {
        RelationshipStoreAccess relStore = getServer().getRelStore();
        if ( rel.getFirstPrevRel() != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            RelationshipRecord prevRel = relStore.forceGetRecord( rel.getFirstPrevRel() );
            if ( prevRel.getFirstNode() == rel.getFirstNode() )
            {
                prevRel.setFirstNextRel( rel.getFirstNextRel() );
            }
            else if ( prevRel.getSecondNode() == rel.getFirstNode() )
            {
                prevRel.setSecondNextRel( rel.getFirstNextRel() );
            }
            else
            {
                throw new RuntimeException(
                    prevRel + " don't match " + rel );
            }
            relStore.forceUpdateRecord( prevRel );
        }
        // update first node next
        if ( rel.getFirstNextRel() != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            RelationshipRecord nextRel = relStore.forceGetRecord( rel.getFirstNextRel() );
            if ( nextRel.getFirstNode() == rel.getFirstNode() )
            {
                nextRel.setFirstPrevRel( rel.getFirstPrevRel() );
            }
            else if ( nextRel.getSecondNode() == rel.getFirstNode() )
            {
                nextRel.setSecondPrevRel( rel.getFirstPrevRel() );
            }
            else
            {
                throw new RuntimeException( nextRel + " don't match "
                    + rel );
            }
            relStore.forceUpdateRecord( nextRel );
        }
        // update second node prev
        if ( rel.getSecondPrevRel() != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            RelationshipRecord prevRel = relStore.forceGetRecord( rel.getSecondPrevRel() );
            if ( prevRel.getFirstNode() == rel.getSecondNode() )
            {
                prevRel.setFirstNextRel( rel.getSecondNextRel() );
            }
            else if ( prevRel.getSecondNode() == rel.getSecondNode() )
            {
                prevRel.setSecondNextRel( rel.getSecondNextRel() );
            }
            else
            {
                throw new RuntimeException( prevRel + " don't match " +
                    rel );
            }
            relStore.forceUpdateRecord( prevRel );
        }
        // update second node next
        if ( rel.getSecondNextRel() != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            RelationshipRecord nextRel = relStore.forceGetRecord( rel.getSecondNextRel() );
            if ( nextRel.getFirstNode() == rel.getSecondNode() )
            {
                nextRel.setFirstPrevRel( rel.getSecondPrevRel() );
            }
            else if ( nextRel.getSecondNode() == rel.getSecondNode() )
            {
                nextRel.setSecondPrevRel( rel.getSecondPrevRel() );
            }
            else
            {
                throw new RuntimeException( nextRel + " don't match " +
                    rel );
            }
            relStore.forceUpdateRecord( nextRel );
        }

        NodeStoreAccess nodeStore = getServer().getNodeStore();
        if ( rel.getFirstPrevRel() == Record.NO_PREV_RELATIONSHIP.intValue() )
        {
            NodeRecord firstNode = nodeStore.forceGetRecord( rel.getFirstNode() );
            firstNode.setNextRel( rel.getFirstNextRel() );
            nodeStore.forceUpdateRecord( firstNode );
        }
        if ( rel.getSecondPrevRel() == Record.NO_PREV_RELATIONSHIP.intValue() )
        {
            NodeRecord secondNode = nodeStore.forceGetRecord( rel.getSecondNode() );
            secondNode.setNextRel( rel.getSecondNextRel() );
            nodeStore.forceUpdateRecord( secondNode );
        }
        rel.setInUse( false );
        relStore.forceUpdateRecord( rel );
    }
}
