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

import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.OptionDefinition;
import org.neo4j.shell.OptionValueType;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;

public class Load extends NioneoApp
{

    public Load()
    {
        super();
        this.addOptionDefinition( "n", new OptionDefinition( OptionValueType.NONE,
            "Load node record" ) );
        this.addOptionDefinition( "r", new OptionDefinition( OptionValueType.NONE,
            "Load rel record" ) );
        this.addOptionDefinition( "p", new OptionDefinition( OptionValueType.NONE,
            "Load property record" ) );
        this.addOptionDefinition( "s", new OptionDefinition( OptionValueType.NONE,
        "Load property record" ) );

    }

    public String execute( AppCommandParser parser, Session session, Output out )
            throws ShellException
    {
        String arg = parser.arguments().get( 0 );
        int id = Integer.parseInt( arg );
        String response = "Record " + id + " loaded";
        if ( parser.options().containsKey( "n" ) )
        {
            getServer().setRecord( getServer().getNodeStore().forceGetRecord( id ) );
        }
        else if ( parser.options().containsKey( "r" ) )
        {
            getServer().setRecord( getServer().getRelStore().forceGetRecord( id ) );
        }
        else if ( parser.options().containsKey( "p" ) )
        {
            getServer().setRecord( getServer().getPropStore().forceGetRecord( id ) );
        }
        else if ( parser.options().containsKey( "s" ) )
        {
            getServer().setRecord( getServer().getStringStore().forceGetRecord( id ) );
        }
        else if ( parser.options().containsKey( "a" ) )
        {
            getServer().setRecord( getServer().getArrayStore().forceGetRecord( id ) );
        }
        else
        {
            response = "Error: Specify type of of record";
        }
        try
        {
            out.println( response );
        }
        catch ( RemoteException e )
        {
            throw new ShellException( e );
        }
        return null;
    }
}
