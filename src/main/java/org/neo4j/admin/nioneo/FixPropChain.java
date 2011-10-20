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

import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeStoreAccess;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyStoreAccess;
import org.neo4j.kernel.impl.nioneo.store.PropertyType;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;

public class FixPropChain extends NioneoApp
{

    public String execute( AppCommandParser parser, Session session, Output out ) throws ShellException
    {
        String arg = parser.arguments().get( 0 );
        int id = Integer.parseInt( arg );
        NodeStoreAccess nodeStore = getServer().getNodeStore();
        NodeRecord nodeRecord = nodeStore.forceGetRecord( id );
        long nextProp = nodeRecord.getNextProp();
        checkChain( getServer(), out, nextProp );
        return null;
    }

    static boolean checkChain( NioneoServer server, Output out, long nextProp ) throws ShellException
    {
        long prevProp = Record.NO_PREVIOUS_PROPERTY.intValue();
        PropertyStoreAccess propStore = server.getPropStore();
        boolean changed = false;
        while ( nextProp != Record.NO_NEXT_PROPERTY.intValue() )
        {
            PropertyRecord propRecord = propStore.forceGetRecord( nextProp );
            if ( !propRecord.inUse() )
            {
                nextProp = Record.NO_NEXT_PROPERTY.intValue();
                if ( prevProp != Record.NO_PREVIOUS_PROPERTY.intValue() )
                {
                    PropertyRecord prev = propStore.forceGetRecord( prevProp );
                    prev.setNextProp( Record.NO_NEXT_PROPERTY.intValue() );
                    propStore.forceUpdateRecord( prev );
                }
                changed = true;
            }
            else
            {
                if ( propRecord.getType() == PropertyType.STRING )
                {
                    DynamicRecord dr = server.getStringStore().forceGetRecord( (int) propRecord.getPropBlock() );
                    if ( !dr.inUse() )
                    {
                        propRecord.setInUse( false );
                        propStore.forceUpdateRecord( propRecord );
                        if ( prevProp != Record.NO_PREVIOUS_PROPERTY.intValue() )
                        {
                            PropertyRecord prev = propStore.forceGetRecord( prevProp );
                            prev.setNextProp( Record.NO_NEXT_PROPERTY.intValue() );
                            propStore.forceUpdateRecord( prev );
                        }
                        changed = true;
                        continue;
                    }
                    if ( dr.getPrevBlock() != Record.NO_PREV_BLOCK.intValue() )
                    {
                        propRecord.setInUse( false );
                        propStore.forceUpdateRecord( propRecord );
                        if ( prevProp != Record.NO_PREVIOUS_PROPERTY.intValue() )
                        {
                            PropertyRecord prev = propStore.forceGetRecord( prevProp );
                            prev.setNextProp( Record.NO_NEXT_PROPERTY.intValue() );
                            propStore.forceUpdateRecord( prev );
                        }
                        changed = true;
                        continue;
                    }
                }
                else if ( propRecord.getType() == PropertyType.ARRAY )
                {
                    DynamicRecord dr = server.getArrayStore().forceGetRecord( (int) propRecord.getPropBlock() );
                    if ( !dr.inUse() )
                    {
                        propRecord.setInUse( false );
                        propStore.forceUpdateRecord( propRecord );
                        if ( prevProp != Record.NO_PREVIOUS_PROPERTY.intValue() )
                        {
                            PropertyRecord prev = propStore.forceGetRecord( prevProp );
                            prev.setNextProp( Record.NO_NEXT_PROPERTY.intValue() );
                            propStore.forceUpdateRecord( prev );
                        }
                        changed = true;
                        continue;
                    }
                    if ( dr.getPrevBlock() != Record.NO_PREV_BLOCK.intValue() )
                    {
                        propRecord.setInUse( false );
                        propStore.forceUpdateRecord( propRecord );
                        if ( prevProp != Record.NO_PREVIOUS_PROPERTY.intValue() )
                        {
                            PropertyRecord prev = propStore.forceGetRecord( prevProp );
                            prev.setNextProp( Record.NO_NEXT_PROPERTY.intValue() );
                            propStore.forceUpdateRecord( prev );
                        }
                        changed = true;
                        continue;
                    }
                }
                if ( propRecord.getPrevProp() != prevProp )
                {
                    propRecord.setPrevProp( prevProp );
                    propStore.forceUpdateRecord( propRecord );
                    changed = true;
                }
                prevProp = propRecord.getId();
                nextProp = propRecord.getNextProp();
            }
        }
//        try
//        {
//            out.println( "Done" );
//        }
//        catch ( RemoteException e )
//        {
//            throw ShellException.wrapCause( e );
//        }
        return changed;
    }
}
