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

public class FixRels extends NioneoApp
{

    public String execute( AppCommandParser parser, Session session, Output out )
            throws ShellException
    {
        for ( int i = 0; i < parser.arguments().size(); i++ )
        {
            String arg = parser.arguments().get( i );
            int id = Integer.parseInt( arg );
            RelationshipStoreAccess relStore = getServer().getRelStore();
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
            throw ShellException.wrapCause( e );
        }
        return null;
    }

    private int getLastRel( int nodeId )
    {
        NodeStoreAccess nodeStore = getServer().getNodeStore();
        NodeRecord nodeRecord = nodeStore.getRecord( nodeId );
        RelationshipStoreAccess relStore = getServer().getRelStore();
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
