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
package org.neo4j.admin.tool.propstat;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.neo4j.admin.tool.kernelprototypes.ShortArray;
import org.neo4j.kernel.impl.nioneo.store.ArrayPropertyStoreAccess;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyStoreAccess;

public class ArrayStatistics extends Statistics
{
    static class LengthAndBitDepth implements Comparable<LengthAndBitDepth>
    {
        private final Integer length;
        private final Integer bitDepth;

        public LengthAndBitDepth( int length, int bitDepth )
        {
            this.length = length;
            this.bitDepth = bitDepth;
        }
        
        @Override
        public String toString()
        {
            return length + "\t" + bitDepth;
        }

        @Override
        public int compareTo( LengthAndBitDepth o )
        {
            int lengthComparison = length.compareTo( o.length );
            return lengthComparison == 0 ? bitDepth.compareTo( o.bitDepth ) : lengthComparison;
        }

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((bitDepth == null) ? 0 : bitDepth.hashCode());
            result = prime * result + ((length == null) ? 0 : length.hashCode());
            return result;
        }

        @Override
        public boolean equals( Object obj )
        {
            if ( this == obj )
                return true;
            if ( obj == null )
                return false;
            if ( getClass() != obj.getClass() )
                return false;
            LengthAndBitDepth other = (LengthAndBitDepth) obj;
            if ( bitDepth == null )
            {
                if ( other.bitDepth != null )
                    return false;
            }
            else if ( !bitDepth.equals( other.bitDepth ) )
                return false;
            if ( length == null )
            {
                if ( other.length != null )
                    return false;
            }
            else if ( !length.equals( other.length ) )
                return false;
            return true;
        }
    }
    
    static LengthAndBitDepth lengthAndBitDepth( int length, int bitDepth )
    {
        return new LengthAndBitDepth( length, bitDepth );
    }
    
    private final ArrayPropertyStoreAccess arrayStore;
    private SortedMap<String, Frequencies<Integer>> lengthFrequenciesForType = new TreeMap<String, Frequencies<Integer>>();
    private SortedMap<String, Frequencies<Integer>> bitDepthFrequenciesForType = new TreeMap<String, Frequencies<Integer>>();
    private SortedMap<String, Frequencies<LengthAndBitDepth>> lengthAndBitDepthFrequenciesForType =
            new TreeMap<String, Frequencies<LengthAndBitDepth>>();
    private int thatFit;
    private int thatDoesntFit;

    public ArrayStatistics( ArrayPropertyStoreAccess arrayStore, PropertyStoreAccess propertyStore )
    {
        super( propertyStore, "arrays" );
        this.arrayStore = arrayStore;
    }

    @Override
    void add( Object value, PropertyRecord record )
    {
        super.add( value, record );
        Class<?> componentTypeClass = value.getClass().getComponentType();
        if ( componentTypeClass == String.class )
        {
            return;
        }
        String componentType = componentTypeClass.toString();
        int length = Array.getLength( value );
        int bitDepth = ShortArray.typeOf( value ).calculateRequiredBitsForArray( value );
        lengthFrequenciesForType( componentType ).record( length );
        bitDepthFrequenciesForType( componentType ).record( bitDepth );
        lengthAndBitDepthFrequenciesForType( componentType ).record( lengthAndBitDepth( length, bitDepth ) );
        
        int bits = length*bitDepth;
        if ( bits <= (24*8)-5 ) thatFit++;
        else thatDoesntFit++;
    }
    
    @Override
    Object extractValue( PropertyRecord record )
    {
        return getPropertyStore().getArrayForDynamicPropertyRecord( record.getId() );
    }

    public int getTotalCount()
    {
        return lengthFrequenciesForType.size();
    }

    public List<Object> getTypes()
    {
        return new ArrayList<Object>( lengthFrequenciesForType.keySet() );
    }

    public Frequencies<Integer> lengthFrequenciesForType( String type )
    {
        Frequencies<Integer> groupedByLength = lengthFrequenciesForType.get( type );
        if ( groupedByLength == null )
            lengthFrequenciesForType.put( type, groupedByLength = new Frequencies<Integer>() );
        return groupedByLength;
    }

    public Frequencies<Integer> bitDepthFrequenciesForType( String type )
    {
        Frequencies<Integer> groupedByLength = bitDepthFrequenciesForType.get( type );
        if ( groupedByLength == null )
            bitDepthFrequenciesForType.put( type, groupedByLength = new Frequencies<Integer>() );
        return groupedByLength;
    }
    
    public Frequencies<LengthAndBitDepth> lengthAndBitDepthFrequenciesForType( String type )
    {
        Frequencies<LengthAndBitDepth> groupedBy = lengthAndBitDepthFrequenciesForType.get( type );
        if ( groupedBy == null )
            lengthAndBitDepthFrequenciesForType.put( type, groupedBy = new Frequencies<LengthAndBitDepth>() );
        return groupedBy;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder( "type\tlength\tbitDepth\tcount\n" );
        for ( Map.Entry<String, Frequencies<LengthAndBitDepth>> entry : lengthAndBitDepthFrequenciesForType.entrySet() )
        {
            builder.append( entry.getValue().toString( entry.getKey() ) ).append( "\n" );
        }
        
        double percentageThatFit = Math.round( (double)thatFit/(double)(thatDoesntFit+thatFit)*100 );
        builder.append( "That fit " + percentageThatFit + "%" );
        
        return builder.toString();
    }

    public static class Frequencies<T>
    {
        private final SortedMap<T, Integer> map = new TreeMap<T, Integer>();

        @SuppressWarnings( "boxing" )
        public void record( T key )
        {
            Integer count = map.get( key );
            if ( count == null )
                count = 0;
            count++;
            map.put( key, count );
        }

        @SuppressWarnings( "boxing" )
        public int frequencyOf( T key )
        {
            return map.get( key );
        }

        public String toString( String prefix )
        {
            StringBuilder builder = new StringBuilder();
            for ( T key : map.keySet() )
            {
                builder.append( prefix ).append( "\t" ).append( key ).append( "\t" )
                        .append( map.get( key ) ).append( "\n" );
            }
            return builder.toString();
        }
    }

    public static int calculateBitDepth( Object array )
    {
        return ShortArray.typeOf( array ).calculateRequiredBitsForArray( array );
    }
}
