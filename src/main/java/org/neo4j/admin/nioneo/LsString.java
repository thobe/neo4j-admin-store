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

import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.StringPropertyStoreAccess;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;

public class LsString extends NioneoApp
{

    public String execute( AppCommandParser parser, Session session, Output out )
            throws ShellException
    {
        String arg = parser.arguments().get( 0 );
        int id = Integer.parseInt( arg );
        StringPropertyStoreAccess stringStore = getServer().getStringStore();
        DynamicRecord record = stringStore.forceGetRecord( id );
        StringBuffer buf = new StringBuffer( "str block #" );
        buf.append( record.getId() ).append( " [" );
        buf.append( record.inUse() );
        buf.append( "|pB " ).append( record.getPrevBlock() );
        buf.append( "|s " ).append( record.getLength() );
        buf.append( "|nB " ).append( record.getNextBlock() );
        buf.append( "|" );
        byte data[] = record.getData();
        for ( int i = 0; i < 3; i++ )
        {
            buf.append( " " ).append( data[i] );
        }
        buf.append( "...]" );
        try
        {
            out.println( buf );
        }
        catch ( RemoteException e )
        {
            throw new ShellException( e );
        }
        return null;
    }

}
