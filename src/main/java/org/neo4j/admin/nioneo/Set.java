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

import org.neo4j.kernel.impl.nioneo.store.AbstractRecord;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.OptionDefinition;
import org.neo4j.shell.OptionValueType;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;

public class Set extends NioneoApp
{

    public Set()
    {
        super();
        this.addOptionDefinition( "v", new OptionDefinition( OptionValueType.MUST,
            "Node set next relationship" ) );
        this.addOptionDefinition( "b", new OptionDefinition( OptionValueType.MUST,
            "Node set next property" ) );

        this.addOptionDefinition( "q", new OptionDefinition( OptionValueType.MUST,
            "Relationship set first prev rel" ) );
        this.addOptionDefinition( "w", new OptionDefinition( OptionValueType.MUST,
            "Relationship set first next rel" ) );
        this.addOptionDefinition( "e", new OptionDefinition( OptionValueType.MUST,
            "Relationship set second prev rel" ) );
        this.addOptionDefinition( "r", new OptionDefinition( OptionValueType.MUST,
            "Relationship set second next rel" ) );
        this.addOptionDefinition( "t", new OptionDefinition( OptionValueType.MUST,
            "Relationship set next prop" ) );

        this.addOptionDefinition( "u", new OptionDefinition( OptionValueType.MUST,
            "Property set block value" ) );
        this.addOptionDefinition( "i", new OptionDefinition( OptionValueType.MUST,
            "Property set prev property" ) );
        this.addOptionDefinition( "o", new OptionDefinition( OptionValueType.MUST,
            "Property set next property" ) );

        this.addOptionDefinition( "k", new OptionDefinition( OptionValueType.MUST,
            "Dynamic record set prev block" ) );
        this.addOptionDefinition( "l", new OptionDefinition( OptionValueType.MUST,
            "Dynamic record length value" ) );
        this.addOptionDefinition( "m", new OptionDefinition( OptionValueType.MUST,
            "Dynamic record set next block" ) );
    }

    public String execute( AppCommandParser parser, Session session, Output out )
            throws ShellException
    {
        AbstractRecord record = getServer().getCurrentRecord();
        String response = "set ok";
        if ( record == null )
        {
            response = "Error: No record loaded";
        }
        else if ( record instanceof NodeRecord )
        {
            NodeRecord rec = (NodeRecord) record;
            String nR = checkIfEndHead( parser.options().get( "v" ) );
            String nP = checkIfEndHead( parser.options().get( "b" ) );

            if ( nR != null )
            {
                rec.setNextRel( Integer.parseInt( nR ) );
            }
            if ( nP != null )
            {
                rec.setNextProp( Integer.parseInt( nP ) );
            }
        }
        else if ( record instanceof RelationshipRecord )
        {
            RelationshipRecord rec = (RelationshipRecord) record;
            String fpR = checkIfEndHead( parser.options().get( "q" ) );
            String fnR = checkIfEndHead( parser.options().get( "w" ) );
            String spR = checkIfEndHead( parser.options().get( "e" ) );
            String snR = checkIfEndHead( parser.options().get( "r" ) );
            String nP = checkIfEndHead( parser.options().get( "t" ) );
            if ( fpR != null )
            {
                rec.setFirstPrevRel( Integer.parseInt( fpR ) );
            }
            if ( fnR != null )
            {
                rec.setFirstNextRel( Integer.parseInt( fnR ) );
            }
            if ( spR != null )
            {
                rec.setSecondPrevRel( Integer.parseInt( spR ) );
            }
            if ( snR != null )
            {
                rec.setSecondNextRel( Integer.parseInt( snR ) );
            }
            if ( nP != null )
            {
                rec.setNextProp( Integer.parseInt( nP ) );
            }
        }
        else if ( record instanceof PropertyRecord )
        {
            PropertyRecord rec = (PropertyRecord) record;
            String value = checkIfEndHead( parser.options().get( "u" ) );
            String pP = checkIfEndHead( parser.options().get( "i" ) );
            String nP = checkIfEndHead( parser.options().get( "o" ) );
            if ( value != null )
            {
                rec.setPropBlock( Long.parseLong( value ) );
            }
            if ( pP != null )
            {
                rec.setPrevProp( Integer.parseInt( pP ) );
            }
            if ( nP != null )
            {
                rec.setNextProp( Integer.parseInt( nP ) );
            }
        }
        else if ( record instanceof DynamicRecord )
        {
            DynamicRecord rec = (DynamicRecord) record;
            String p = checkIfEndHead( parser.options().get( "k" ) );
            String l = checkIfEndHead( parser.options().get( "l" ) );
            String n = checkIfEndHead( parser.options().get( "m" ) );
            if ( p != null )
            {
                rec.setPrevBlock( Integer.parseInt( p ) );
            }
            if ( l != null )
            {
                rec.setLength( Integer.parseInt( l ) );
            }
            if ( n != null )
            {
                rec.setNextBlock( Integer.parseInt( n ) );
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
            throw new ShellException( e );
        }
        return null;
    }

    private String checkIfEndHead( String str )
    {
        if ( str == null )
        {
            return null;
        }
        if ( str.toLowerCase().trim().equals( "end" ) )
        {
            return "-1";
        }
        return str;
    }
}
