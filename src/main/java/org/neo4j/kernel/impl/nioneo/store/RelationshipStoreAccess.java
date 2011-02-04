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

public class RelationshipStoreAccess extends StoreAccess<RelationshipStore, RelationshipRecord>
{
    RelationshipStoreAccess( RelationshipStore store )
    {
        super( store );
    }

    public RelationshipRecord getRecord( int id )
    {
        return store.getRecord( id );
    }

    @Override
    public RelationshipRecord forceGetRecord( int id )
    {
        PersistenceWindow window = store.acquireWindow( id, OperationType.READ );
        try
        {
            Buffer buffer = window.getOffsettedBuffer( id );
            byte inUse = buffer.get();
            boolean inUseFlag = ( ( inUse & Record.IN_USE.byteValue() ) == Record.IN_USE.byteValue() );
            RelationshipRecord record = new RelationshipRecord( id, buffer.getInt(), buffer.getInt(), buffer.getInt() );
            record.setInUse( inUseFlag );
            record.setFirstPrevRel( buffer.getInt() );
            record.setFirstNextRel( buffer.getInt() );
            record.setSecondPrevRel( buffer.getInt() );
            record.setSecondNextRel( buffer.getInt() );
            record.setNextProp( buffer.getInt() );
            return record;
        }
        finally
        {
            store.releaseWindow( window );
        }
    }

    public void forceUpdateRecord( RelationshipRecord record )
    {
        PersistenceWindow window = store.acquireWindow( record.getId(), OperationType.WRITE );
        try
        {
            int id = record.getId();
            Buffer buffer = window.getOffsettedBuffer( id );
            byte inUse = record.inUse() ? Record.IN_USE.byteValue() : Record.NOT_IN_USE.byteValue();
            buffer.put( inUse ).putInt( record.getFirstNode() ).putInt( record.getSecondNode() ).putInt(
                    record.getType() ).putInt( record.getFirstPrevRel() ).putInt( record.getFirstNextRel() ).putInt(
                    record.getSecondPrevRel() ).putInt( record.getSecondNextRel() ).putInt( record.getNextProp() );
        }
        finally
        {
            store.releaseWindow( window );
        }
    }
}
