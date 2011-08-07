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

import org.neo4j.kernel.impl.nioneo.store.Abstract64BitRecord;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyType;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;

public class Store extends NioneoApp
{

    public String execute( AppCommandParser parser, Session session, Output out )
            throws ShellException
    {
        Abstract64BitRecord record = getServer().getCurrentRecord();
        String response;
        if ( record == null )
        {
            response = "Error: No record loaded";
        }
        else if ( record instanceof NodeRecord )
        {
            getServer().getNodeStore().forceUpdateRecord( (NodeRecord) record );
            response = "NodeRecord #" + record.getId() + " stored";
        }
        else if ( record instanceof RelationshipRecord )
        {
            getServer().getRelStore().forceUpdateRecord( (RelationshipRecord) record );
            response = "RelRecord #" + record.getId() + " stored";
        }
        else if ( record instanceof PropertyRecord )
        {
            getServer().getPropStore().forceUpdateRecord( (PropertyRecord) record );
            response = "PropRecord #" + record.getId() + " stored";
        }
        else if ( record instanceof DynamicRecord )
        {
            DynamicRecord rec = (DynamicRecord )record;
            if ( rec.getType() == PropertyType.STRING.intValue() )
            {
                getServer().getStringStore().forceUpdateRecord( rec );
                response = "StringRecord #" + record.getId() + " stored";

            }
            else if ( rec.getType() == PropertyType.ARRAY.intValue() )
            {
                getServer().getArrayStore().forceUpdateRecord( rec );
                response = "ArrayRecord #" + record.getId() + " stored";

            }
            else
            {
                response = "Error: unkown record type " + record;
            }
        }
        else
        {
            response = "Error: unkown record type " + record;
        }
        try
        {
            out.println( response );
        }
        catch ( RemoteException e )
        {
            throw ShellException.wrapCause( e );
        }
        return null;
    }
}
