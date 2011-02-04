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
package org.neo4j.kernel.impl.nioneo.store;

public class PropertyStoreAccess extends StoreAccess<PropertyStore, PropertyRecord>
{
    PropertyStoreAccess( PropertyStore store )
    {
        super( store );
    }

    @Override
    public PropertyRecord forceGetRecord( int id )
    {
        PersistenceWindow window = store.acquireWindow( id, OperationType.READ );
        try
        {
            Buffer buffer = window.getOffsettedBuffer( id );
            boolean inUse = buffer.get() == Record.IN_USE.byteValue() ? true : false;
            PropertyRecord record = new PropertyRecord( id );
            record.setType( GraphDatabaseStore.getEnumTypeSafe( buffer.getInt() ) );
            record.setInUse( inUse );
            record.setKeyIndexId( buffer.getInt() );
            record.setPropBlock( buffer.getLong() );
            record.setPrevProp( buffer.getInt() );
            record.setNextProp( buffer.getInt() );
            return record;
        }
        finally
        {
            store.releaseWindow( window );
        }
    }

    public void forceUpdateRecord( PropertyRecord record )
    {
        PersistenceWindow window = store.acquireWindow( record.getId(), OperationType.WRITE );
        try
        {
            int id = record.getId();
            Buffer buffer = window.getOffsettedBuffer( id );
            buffer.put( record.inUse() ? Record.IN_USE.byteValue() : Record.NOT_IN_USE.byteValue() ).putInt(
                    record.getType().intValue() ).putInt( record.getKeyIndexId() ).putLong( record.getPropBlock() ).putInt(
                    record.getPrevProp() ).putInt( record.getNextProp() );
        }
        finally
        {
            store.releaseWindow( window );
        }
    }
}
