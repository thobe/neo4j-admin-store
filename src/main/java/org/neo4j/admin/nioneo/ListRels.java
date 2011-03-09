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

public class ListRels extends NioneoApp
{

    public String execute( AppCommandParser parser, Session session, Output out )
            throws ShellException
    {
        String arg = parser.arguments().get( 0 );
        int nodeId = Integer.parseInt( arg );
        NodeStoreAccess nodeStore = getServer().getNodeStore();
        NodeRecord nodeRecord = nodeStore.getRecord( nodeId );
        RelationshipStoreAccess relStore = getServer().getRelStore();
        long nextRelId = nodeRecord.getNextRel();
        long prevRelId = -1;
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
            throw ShellException.wrapCause( e );
        }
        return null;
    }
}
