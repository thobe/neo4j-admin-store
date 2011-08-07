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


public class NodeStoreAccess extends StoreAccess<NodeStore, NodeRecord> implements PropertyContainerStore<NodeRecord>
{
    NodeStoreAccess( NodeStore store )
    {
        super( store );
    }

    @Override
    public NodeRecord copy( NodeRecord source, long newId )
    {
        NodeRecord target = new NodeRecord( newId );
        target.setNextRel( source.getNextRel() );
        target.setNextProp( source.getNextProp() );
        return target;
    }

    public NodeRecord getRecord( long id )
    {
        return store.getRecord( id );
    }

    @Override
    public NodeRecord forceGetRecord( long id )
    {
        PersistenceWindow window = store.acquireWindow( id, OperationType.READ );
        try
        {
            Buffer buffer = window.getOffsettedBuffer( id );

            // [    ,   x] in use bit
            // [    ,xxx ] higher bits for rel id
            // [xxxx,    ] higher bits for prop id
            long inUseByte = buffer.get();

            boolean inUse = (inUseByte & 0x1) == Record.IN_USE.intValue();

            long nextRel = buffer.getUnsignedInt();
            long nextProp = buffer.getUnsignedInt();

            long relModifier = (inUseByte & 0xEL) << 31;
            long propModifier = (inUseByte & 0xF0L) << 28;

            NodeRecord nodeRecord = new NodeRecord( id );
            nodeRecord.setInUse( inUse );
            nodeRecord.setNextRel( longFromIntAndMod( nextRel, relModifier ) );
            nodeRecord.setNextProp( longFromIntAndMod( nextProp, propModifier ) );
            return nodeRecord;
        }
        finally
        {
            store.releaseWindow( window );
        }
    }

    @Override
    public void forceUpdateRecord( NodeRecord record )
    {
        PersistenceWindow window = store.acquireWindow( record.getId(), OperationType.WRITE );
        try
        {
            long id = record.getId();
            Buffer buffer = window.getOffsettedBuffer( id );
            long nextRel = record.getNextRel();
            long nextProp = record.getNextProp();

            short relModifier = nextRel == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (short)((nextRel & 0x700000000L) >> 31);
            short propModifier = nextProp == Record.NO_NEXT_PROPERTY.intValue() ? 0 : (short)((nextProp & 0xF00000000L) >> 28);

            // [    ,   x] in use bit
            // [    ,xxx ] higher bits for rel id
            // [xxxx,    ] higher bits for prop id
            short inUseUnsignedByte = Record.IN_USE.byteValue();
            if ( !record.inUse() )
            {
                inUseUnsignedByte = 0;
            }
            inUseUnsignedByte = (short)(inUseUnsignedByte | relModifier | propModifier);
            buffer.put( (byte)inUseUnsignedByte ).putInt( (int) nextRel ).putInt( (int) nextProp );
        }
        finally
        {
            store.releaseWindow( window );
        }
    }

    @Override
    public void setFirstPropertyOf( NodeRecord record, long property )
    {
        record.setNextProp( property );
    }

    @Override
    public long getFirstPropertyOf( NodeRecord record )
    {
        return record.getNextProp();
    }
}
