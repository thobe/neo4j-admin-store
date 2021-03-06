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

public class CheckRels extends NioneoApp
{

    public String execute( AppCommandParser parser, Session session, Output out )
            throws ShellException
    {
        NodeStoreAccess nodeStore = getServer().getNodeStore();
        RelationshipStoreAccess relStore = getServer().getRelStore();
        int maxRelId = (int) relStore.getHighId();
        int count = 0 ;
        for ( int i = 0; i < maxRelId; i++ )
        {
            RelationshipRecord record = relStore.forceGetRecord( i );
            if ( !record.inUse() )
            {
                continue;
            }
            NodeRecord firstNode = nodeStore.forceGetRecord( record.getFirstNode() );
            NodeRecord secondNode = nodeStore.forceGetRecord( record.getSecondNode() );
            if ( !firstNode.inUse() )
            {
                relDelete( record );
                count++;
            }
            else if ( !secondNode.inUse() )
            {
                relDelete( record );
                count++;
            }
            else
            {
//                if ( record.getFirstPrevRel() != Record.NO_PREV_RELATIONSHIP.intValue() )
//                {
//                    RelationshipRecord prevRel = relStore.forceGetRecord( record.getFirstPrevRel() );
//                    if ( !prevRel.inUse() )
//                    {
//                        relDelete( record );
//                    }
//                    else
//                    {
//                        if ( prevRel.getFirstNode() == record.getFirstNode() &&
//                            prevRel.getFirstNextRel() != record.getId() )
//                        {
//                            relDelete( record );
//                            count++;
//                        }
//                        else if ( prevRel.getSecondNode() == record.getFirstNode() &&
//                            prevRel.getSecondNextRel() != record.getId() )
//                        {
//                            relDelete( record );
//                            count++;
//                        }
//                    }
//                }
//                if ( record.inUse() && record.getSecondPrevRel() != Record.NO_PREV_RELATIONSHIP.intValue() )
//                {
//                    RelationshipRecord prevRel = relStore.forceGetRecord( record.getSecondPrevRel() );
//                    if ( !prevRel.inUse() )
//                    {
//                        relDelete( record );
//                    }
//                    else
//                    {
//                        if ( prevRel.getFirstNode() == record.getSecondNode() &&
//                            prevRel.getFirstNextRel() != record.getId() )
//                        {
//                            relDelete( record );
//                            count++;
//                        }
//                        else if ( prevRel.getSecondNode() == record.getSecondNode() &&
//                            prevRel.getSecondNextRel() != record.getId() )
//                        {
//                            relDelete( record );
//                            count++;
//                        }
//                    }
//                }
//                if ( record.inUse() && record.getFirstNextRel() != Record.NO_NEXT_RELATIONSHIP.intValue() )
//                {
//                    RelationshipRecord nextRel = relStore.forceGetRecord( record.getFirstNextRel() );
//                    if ( !nextRel.inUse() )
//                    {
//                        relDelete( record );
//                    }
//                    else
//                    {
//                        if ( nextRel.getFirstNode() == record.getFirstNode() &&
//                            nextRel.getFirstPrevRel() != record.getId() )
//                        {
//                            relDelete( record );
//                            count++;
//                        }
//                        else if ( nextRel.getSecondNode() == record.getFirstNode() &&
//                            nextRel.getSecondPrevRel() != record.getId() )
//                        {
//                            relDelete( record );
//                            count++;
//                        }
//                    }
//                }
//                if ( record.inUse() && record.getSecondNextRel() != Record.NO_NEXT_RELATIONSHIP.intValue() )
//                {
//                    RelationshipRecord nextRel = relStore.forceGetRecord( record.getSecondNextRel() );
//                    if ( !nextRel.inUse() )
//                    {
//                        relDelete( record );
//                    }
//                    else
//                    {
//                        if ( nextRel.getFirstNode() == record.getSecondNode() &&
//                            nextRel.getFirstPrevRel() != record.getId() )
//                        {
//                            relDelete( record );
//                            count++;
//                        }
//                        else if ( nextRel.getSecondNode() == record.getSecondNode() &&
//                            nextRel.getSecondPrevRel() != record.getId() )
//                        {
//                            relDelete( record );
//                            count++;
//                        }
//                    }
//                }
            }
        }
        try
        {
            out.println( count + " rels deleted" );
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

        NodeStoreAccess nodeStore = getServer().getNodeStore();
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
