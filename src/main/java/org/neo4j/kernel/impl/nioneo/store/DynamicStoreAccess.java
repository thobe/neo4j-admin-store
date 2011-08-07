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

@SuppressWarnings( "boxing" )
public abstract class DynamicStoreAccess<T extends AbstractDynamicStore> extends StoreAccess<T, DynamicRecord>
        implements ChainStore<DynamicRecord>
{
    public static final long NO_NEXT_RECORD = GraphDatabaseStore.NO_NEXT_BLOCK,
            NO_PREV_RECORD = GraphDatabaseStore.NO_PREV_BLOCK;
    private static final int BLOCK_HEADER_SIZE;
    static
    {
        BLOCK_HEADER_SIZE = (Integer) GraphDatabaseStore.get( AbstractDynamicStore.class, "BLOCK_HEADER_SIZE", null );
    }

    public void makeHeavy( DynamicRecord record )
    {
        store.makeHeavy( record );
    }

    DynamicStoreAccess( T store )
    {
        super( store );
    }

    @Override
    public DynamicRecord copy( DynamicRecord source, long newId )
    {
        DynamicRecord target = new DynamicRecord( newId );
        target.setType( source.getType() );
        target.setLength( source.getLength() );
        target.setNextBlock( source.getNextBlock() );
        target.setPrevBlock( source.getPrevBlock() );
        if ( source.isLight() )
        {
            makeHeavy( source );
        }
        if ( source.isCharData() )
        {
            target.setCharData( source.getDataAsChar() );
        }
        else
        {
            target.setData( source.getData() );
        }
        return target;
    }

    @Override
    public DynamicRecord forceGetRecord( long blockId )
    {
        PersistenceWindow window = store.acquireWindow( blockId, OperationType.READ );
        try
        {
            DynamicRecord record = new DynamicRecord( blockId );
            Buffer buffer = window.getOffsettedBuffer( blockId );

            // [ , x] in use
            // [xxxx, ] high bits for prev block
            long inUseByte = buffer.get();
            boolean inUse = ( inUseByte & 0x1 ) == Record.IN_USE.intValue();
            long prevBlock = buffer.getUnsignedInt();
            long prevModifier = ( inUseByte & 0xF0L ) << 28;

            int dataSize = store.getBlockSize() - BLOCK_HEADER_SIZE;

            // [ , ][xxxx,xxxx][xxxx,xxxx][xxxx,xxxx] number of bytes
            // [ ,xxxx][ , ][ , ][ , ] higher bits for next block
            long nrOfBytesInt = buffer.getInt();

            int nrOfBytes = (int) ( nrOfBytesInt & 0xFFFFFF );

            long nextBlock = buffer.getUnsignedInt();
            long nextModifier = ( nrOfBytesInt & 0xF000000L ) << 8;

            long longNextBlock = longFromIntAndMod( nextBlock, nextModifier );

            byte byteArrayElement[] = new byte[dataSize];
            buffer.get( byteArrayElement );
            record.setData( byteArrayElement );

            record.setInUse( inUse );
            record.setLength( nrOfBytes );
            record.setPrevBlock( longFromIntAndMod( prevBlock, prevModifier ) );
            record.setNextBlock( longNextBlock );
            return record;
        }
        finally
        {
            store.releaseWindow( window );
        }
    }

    @Override
    public void forceUpdateRecord( DynamicRecord record )
    {
        long blockId = record.getId();
        PersistenceWindow window = store.acquireWindow( blockId, OperationType.WRITE );
        try
        {
            Buffer buffer = window.getOffsettedBuffer( blockId );

            long prevProp = record.getPrevBlock();
            short prevModifier = prevProp == Record.NO_NEXT_BLOCK.intValue() ? 0
                    : (short) ( ( prevProp & 0xF00000000L ) >> 28 );

            long nextProp = record.getNextBlock();
            int nextModifier = nextProp == Record.NO_NEXT_BLOCK.intValue() ? 0
                    : (int) ( ( nextProp & 0xF00000000L ) >> 8 );

            // [ , x] in use
            // [xxxx, ] high prev block bits
            short inUseUnsignedByte = Record.IN_USE.byteValue();
            if ( !record.inUse() )
            {
                inUseUnsignedByte = 0;
            }
            inUseUnsignedByte = (short) ( inUseUnsignedByte | prevModifier );

            // [ , ][xxxx,xxxx][xxxx,xxxx][xxxx,xxxx] nr of bytes
            // [ ,xxxx][ , ][ , ][ , ] high next block bits
            int nrOfBytesInt = record.getLength();
            nrOfBytesInt |= nextModifier;

            assert record.getId() != record.getPrevBlock();
            buffer.put( (byte) inUseUnsignedByte ).putInt( (int) prevProp ).putInt( nrOfBytesInt )
                    .putInt( (int) nextProp );

            if ( record.inUse() && !record.isLight() )
            {
                if ( !record.isCharData() )
                {
                    buffer.put( record.getData() );
                }
                else
                {
                    buffer.put( record.getDataAsChar() );
                }
            }
        }
        finally
        {
            store.releaseWindow( window );
        }
    }

    public Iterable<DynamicRecord> chain( long firstId )
    {
        return new RecordChain<DynamicRecord, DynamicStoreAccess<T>>( this, firstId );
    }

    @Override
    public DynamicRecord nextRecordInChainOrNull( DynamicRecord prev )
    {
        long next = prev.getNextBlock();
        return ( next == NO_NEXT_RECORD ) ? null : forceGetRecord( next );
    }

    @Override
    public void linkChain( DynamicRecord prev, DynamicRecord next )
    {
        prev.setNextBlock( next.getId() );
        next.setPrevBlock( prev.getId() );
    }
}
