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
import java.util.HashSet;
import java.util.Set;

import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeStoreAccess;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipStoreAccess;
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
        RelationshipStoreAccess relStore = getServer().getRelStore();
        NodeStoreAccess nodeStore = getServer().getNodeStore();
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
            throw ShellException.wrapCause( e );
        }
        return null;
    }
}
