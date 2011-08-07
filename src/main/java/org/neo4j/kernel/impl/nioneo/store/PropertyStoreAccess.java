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


public class PropertyStoreAccess extends StoreAccess<PropertyStore, PropertyRecord> implements
        ChainStore<PropertyRecord>
{
    public static final long NO_NEXT_RECORD = GraphDatabaseStore.NO_NEXT_PROP,
            NO_PREV_RECORD = GraphDatabaseStore.NO_PREV_PROP;

    PropertyStoreAccess( PropertyStore store )
    {
        super( store );
    }

    public void close()
    {
        store.close();
    }

    @Override
    public PropertyRecord copy( PropertyRecord source, long newId )
    {
        PropertyRecord target = new PropertyRecord( newId );
        target.setType( source.getType() );
        target.setKeyIndexId( source.getKeyIndexId() );
        target.setNextProp( source.getNextProp() );
        target.setPrevProp( source.getPrevProp() );
        target.setPropBlock( source.getPropBlock() );
        return target;
    }

    @Override
    public PropertyRecord forceGetRecord( long id )
    {
        PersistenceWindow window = store.acquireWindow( id, OperationType.READ );
        try
        {
            Buffer buffer = window.getOffsettedBuffer( id );

            // [    ,   x] in use
            // [xxxx,    ] high prev prop bits
            long inUseByte = buffer.get();

            boolean inUse = (inUseByte & 0x1) == Record.IN_USE.intValue();
            PropertyRecord record = new PropertyRecord( id );

            // [    ,    ][    ,    ][xxxx,xxxx][xxxx,xxxx] type
            // [    ,    ][    ,xxxx][    ,    ][    ,    ] high next prop bits
            long typeInt = buffer.getInt();

            record.setType( getEnumType( (int)typeInt & 0xFFFF ) );
            record.setInUse( inUse );
            record.setKeyIndexId( buffer.getInt() );
            record.setPropBlock( buffer.getLong() );

            long prevProp = buffer.getUnsignedInt();
            long prevModifier = (inUseByte & 0xF0L) << 28;
            long nextProp = buffer.getUnsignedInt();
            long nextModifier = (typeInt & 0xF0000L) << 16;

            record.setPrevProp( longFromIntAndMod( prevProp, prevModifier ) );
            record.setNextProp( longFromIntAndMod( nextProp, nextModifier ) );
            return record;
        }
        finally
        {
            store.releaseWindow( window );
        }
    }

    @Override
    public void forceUpdateRecord( PropertyRecord record )
    {
        PersistenceWindow window = store.acquireWindow( record.getId(), OperationType.WRITE );
        try
        {
            long id = record.getId();
            Buffer buffer = window.getOffsettedBuffer( id );
            long prevProp = record.getPrevProp();
            long prevModifier = prevProp == Record.NO_NEXT_PROPERTY.intValue() ? 0 : (prevProp & 0xF00000000L) >> 28;

            long nextProp = record.getNextProp();
            long nextModifier = nextProp == Record.NO_NEXT_PROPERTY.intValue() ? 0 : (nextProp & 0xF00000000L) >> 16;

            // [    ,   x] in use
            // [xxxx,    ] high prev prop bits
            short inUseUnsignedByte = Record.IN_USE.byteValue();
            if ( !record.inUse() )
            {
                inUseUnsignedByte = 0;
            }
            inUseUnsignedByte = (short)(inUseUnsignedByte | prevModifier);

            // [    ,    ][    ,    ][xxxx,xxxx][xxxx,xxxx] type
            // [    ,    ][    ,xxxx][    ,    ][    ,    ] high next prop bits
            int typeInt = record.getType().intValue();
            typeInt |= nextModifier;

            buffer.put( (byte)inUseUnsignedByte ).putInt( typeInt )
                .putInt( record.getKeyIndexId() ).putLong( record.getPropBlock() )
                .putInt( (int) prevProp ).putInt( (int) nextProp );
        }
        finally
        {
            store.releaseWindow( window );
        }
    }

    private PropertyType getEnumType( int type )
    {
        PropertyType result;
        try
        {
            result = PropertyType.getPropertyType( type, true );
            if ( result == null ) result = PropertyType.ILLEGAL;
        }
        catch ( InvalidRecordException ex )
        {
            result = null;
        }
        return result;
    }

    public Iterable<PropertyRecord> chain( long firstId )
    {
        return new RecordChain<PropertyRecord, PropertyStoreAccess>( this, firstId );
    }

    @Override
    public PropertyRecord nextRecordInChainOrNull( PropertyRecord prev )
    {
        long next = prev.getNextProp();
        return ( next == NO_NEXT_RECORD ) ? null : forceGetRecord( next );
    }

    @Override
    public void linkChain( PropertyRecord prev, PropertyRecord next )
    {
        prev.setNextProp( next.getId() );
        next.setPrevProp( prev.getId() );
    }
}
