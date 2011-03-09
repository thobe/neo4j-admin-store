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

    public RelationshipRecord getRecord( long id )
    {
        return store.getRecord( id );
    }

    @Override
    public RelationshipRecord forceGetRecord( long id )
    {
        PersistenceWindow window = store.acquireWindow( id, OperationType.READ );
        try
        {
        Buffer buffer = window.getOffsettedBuffer( id );

        // [    ,   x] in use flag
        // [    ,xxx ] first node high order bits
        // [xxxx,    ] next prop high order bits
        long inUseByte = buffer.get();

        boolean inUse = (inUseByte & 0x1) == Record.IN_USE.intValue();

        long firstNode = buffer.getUnsignedInt();
        long firstNodeMod = (inUseByte & 0xEL) << 31;

        long secondNode = buffer.getUnsignedInt();

        // [ xxx,    ][    ,    ][    ,    ][    ,    ] second node high order bits,     0x70000000
        // [    ,xxx ][    ,    ][    ,    ][    ,    ] first prev rel high order bits,  0xE000000
        // [    ,   x][xx  ,    ][    ,    ][    ,    ] first next rel high order bits,  0x1C00000
        // [    ,    ][  xx,x   ][    ,    ][    ,    ] second prev rel high order bits, 0x380000
        // [    ,    ][    , xxx][    ,    ][    ,    ] second next rel high order bits, 0x70000
        // [    ,    ][    ,    ][xxxx,xxxx][xxxx,xxxx] type
        long typeInt = buffer.getInt();
        long secondNodeMod = (typeInt & 0x70000000L) << 4;
        int type = (int)(typeInt & 0xFFFF);

        RelationshipRecord record = new RelationshipRecord( id,
            longFromIntAndMod( firstNode, firstNodeMod ),
            longFromIntAndMod( secondNode, secondNodeMod ), type );
        record.setInUse( inUse );

        long firstPrevRel = buffer.getUnsignedInt();
        long firstPrevRelMod = (typeInt & 0xE000000L) << 7;
        record.setFirstPrevRel( longFromIntAndMod( firstPrevRel, firstPrevRelMod ) );

        long firstNextRel = buffer.getUnsignedInt();
        long firstNextRelMod = (typeInt & 0x1C00000L) << 10;
        record.setFirstNextRel( longFromIntAndMod( firstNextRel, firstNextRelMod ) );

        long secondPrevRel = buffer.getUnsignedInt();
        long secondPrevRelMod = (typeInt & 0x380000L) << 13;
        record.setSecondPrevRel( longFromIntAndMod( secondPrevRel, secondPrevRelMod ) );

        long secondNextRel = buffer.getUnsignedInt();
        long secondNextRelMod = (typeInt & 0x70000L) << 16;
        record.setSecondNextRel( longFromIntAndMod( secondNextRel, secondNextRelMod ) );

        long nextProp = buffer.getUnsignedInt();
        long nextPropMod = (inUseByte & 0xF0L) << 28;

        record.setNextProp( longFromIntAndMod( nextProp, nextPropMod ) );
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
        long id = record.getId();
        Buffer buffer = window.getOffsettedBuffer( id );
        long firstNode = record.getFirstNode();
        short firstNodeMod = (short)((firstNode & 0x700000000L) >> 31);

        long secondNode = record.getSecondNode();
        long secondNodeMod = (secondNode & 0x700000000L) >> 4;

        long firstPrevRel = record.getFirstPrevRel();
        long firstPrevRelMod = firstPrevRel == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (firstPrevRel & 0x700000000L) >> 7;

        long firstNextRel = record.getFirstNextRel();
        long firstNextRelMod = firstNextRel == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (firstNextRel & 0x700000000L) >> 10;

        long secondPrevRel = record.getSecondPrevRel();
        long secondPrevRelMod = secondPrevRel == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (secondPrevRel & 0x700000000L) >> 13;

        long secondNextRel = record.getSecondNextRel();
        long secondNextRelMod = secondNextRel == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (secondNextRel & 0x700000000L) >> 16;

        long nextProp = record.getNextProp();
        long nextPropMod = nextProp == Record.NO_NEXT_PROPERTY.intValue() ? 0 : (nextProp & 0xF00000000L) >> 28;

        // [    ,   x] in use flag
        // [    ,xxx ] first node high order bits
        // [xxxx,    ] next prop high order bits
        short inUseUnsignedByte = Record.IN_USE.byteValue();
        if ( !record.inUse() )
        {
            inUseUnsignedByte = 0;
        }
        inUseUnsignedByte = (short)(inUseUnsignedByte | firstNodeMod | nextPropMod);

        // [ xxx,    ][    ,    ][    ,    ][    ,    ] second node high order bits,     0x70000000
        // [    ,xxx ][    ,    ][    ,    ][    ,    ] first prev rel high order bits,  0xE000000
        // [    ,   x][xx  ,    ][    ,    ][    ,    ] first next rel high order bits,  0x1C00000
        // [    ,    ][  xx,x   ][    ,    ][    ,    ] second prev rel high order bits, 0x380000
        // [    ,    ][    , xxx][    ,    ][    ,    ] second next rel high order bits, 0x70000
        // [    ,    ][    ,    ][xxxx,xxxx][xxxx,xxxx] type
        int typeInt = (int)(record.getType() | secondNodeMod | firstPrevRelMod | firstNextRelMod | secondPrevRelMod | secondNextRelMod);

        buffer.put( (byte)inUseUnsignedByte ).putInt( (int) firstNode ).putInt( (int) secondNode )
            .putInt( typeInt ).putInt( (int) firstPrevRel ).putInt( (int) firstNextRel )
            .putInt( (int) secondPrevRel ).putInt( (int) secondNextRel ).putInt( (int) nextProp );

        }
        finally
        {
            store.releaseWindow( window );
        }
    }
}