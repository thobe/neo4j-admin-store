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

import org.neo4j.admin.nioneo.store.Record;
import org.neo4j.admin.nioneo.store.RelationshipRecord;
import org.neo4j.admin.nioneo.store.RelationshipStore;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;

public class FindStartRelForRelNode extends NioneoApp
{

    public String execute( AppCommandParser parser, Session session, Output out )
            throws ShellException
    {
        String arg = parser.arguments().get( 0 );
        int relId = Integer.parseInt( arg );
        int nodeId = Integer.parseInt( parser.arguments().get( 1 ) );
        RelationshipStore relStore = getServer().getRelStore();
        String hit = "Not found";
        int nextRelId = relId;
        int prevRelId = -1;
        boolean error = false;
        do
        {
            RelationshipRecord record = relStore.getRecord( nextRelId );
            if ( record.getFirstNode() == nodeId )
            {
                prevRelId = nextRelId;
                nextRelId = record.getFirstPrevRel();
            }
            else if ( record.getSecondNode() == nodeId )
            {
                prevRelId = nextRelId;
                nextRelId = record.getSecondPrevRel();
            }
            else
            {
                hit = "Error going from rel " + prevRelId + " to " + nextRelId;
                nextRelId = Record.NO_PREV_RELATIONSHIP.intValue();
                error = true;
            }
        } while ( nextRelId != Record.NO_PREV_RELATIONSHIP.intValue() );
        if ( !error )
        {
            hit = "rel record # " + prevRelId;
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
