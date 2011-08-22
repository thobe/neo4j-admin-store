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

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.Map;
import java.util.TreeMap;

import org.neo4j.helpers.Pair;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyStoreAccess;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.ShortStringEncoding;
import org.neo4j.kernel.impl.nioneo.store.StringPropertyStoreAccess;

abstract class StringStatistics extends Statistics
{
    private static final int NO_NEXT_BLOCK = Record.NO_NEXT_BLOCK.intValue();

    static class ShortString extends StringStatistics
    {
        @Override
        void add( Object value, PropertyRecord record )
        {
            super.add( value, record );
            statistics.get( ShortStringEncoding.getEncoding( record.getPropBlock() ) ).add( value, record );
        }
        
        @Override
        Object extractValue( PropertyRecord record )
        {
            return ShortStringEncoding.extractValue( record );
        }

        @Override
        String header()
        {
            return "Short string properties";
        }
    }

    static class DynamicString extends StringStatistics
    {
        private long singleBlock = 0, multiBlock = 0;
        private final StringPropertyStoreAccess stringStore;
        private final Map<Integer, Pair<Statistics, Collection<String>>> longStringByLengthStatistics =
                new TreeMap<Integer, Pair<Statistics, Collection<String>>>();
        private static final int[] LENGTH_CATEGORIES = new int[] { 5, 10, 15, 20, 25, 30, 50, 100, 200, 500 };
        private final PropertyStoreAccess propertyStoreAccess;
        private final boolean useLongVersion;

        DynamicString( StringPropertyStoreAccess stringStore, PropertyStoreAccess propertyStoreAccess, boolean useLongVersion )
        {
            this.stringStore = stringStore;
            this.propertyStoreAccess = propertyStoreAccess;
            this.useLongVersion = useLongVersion;
            int previous = 0;
            for ( int category : LENGTH_CATEGORIES )
            {
                longStringByLengthStatistics.put( category, Pair.<Statistics, Collection<String>>of(
                        new Statistics( propertyStoreAccess, "Dynamic string length " + previous + "-" + (category-1) ), new ArrayList<String>() ) );
                previous = category;
            }
            longStringByLengthStatistics.put( Integer.MAX_VALUE, Pair.<Statistics, Collection<String>>of(
                    new Statistics( propertyStoreAccess, "Dynamic string length " + previous + "-" ), new ArrayList<String>() ) );
        }
        
        @Override
        Object extractValue( PropertyRecord record )
        {
            DynamicRecord dynamicRecord = stringStore.forceGetRecord( record.getPropBlock() );
            if ( !dynamicRecord.inUse() )
            {
                return null;
            }
            
            return propertyStoreAccess.getStringForDynamicPropertyRecord( record.getId() );
        }

        @Override
        void add( Object value, PropertyRecord record )
        {
            if ( value == null )
            {
                return;
            }
            
            super.add( value, record );
            String stringValue = (String) value;
            ShortStringEncoding encoding = ShortStringEncoding.getEncoding( stringValue, useLongVersion );
            if ( encoding != null )
            {
                statistics.get( encoding ).add( null, null );
            }
            else
            {
                DynamicRecord dynamicRecord = stringStore.forceGetRecord( record.getPropBlock() );
                if ( dynamicRecord.getNextBlock() == NO_NEXT_BLOCK ) singleBlock++;
                else multiBlock++;
                addStringByLengthToStats( stringValue );
            }
        }

        private void addStringByLengthToStats( String string )
        {
            int category = getLengthCategory( string );
            Pair<Statistics, Collection<String>> stats = longStringByLengthStatistics.get( category );
            stats.first().add( null, null );
            if ( stats.other().size() < 5 )
            {
                stats.other().add( string );
            }
        }

        private int getLengthCategory( String string )
        {
            int stringLength = string.length();
            for ( int category : LENGTH_CATEGORIES )
            {
                if ( stringLength < category )
                {
                    return category;
                }
            }
            return Integer.MAX_VALUE;
        }

        @Override
        @SuppressWarnings( "boxing" )
        String header()
        {
            int percentageThatCouldHaveBeenShort = (int) Math.round( ((double)shortCount()/(double)allCount())*100 );
            return String.format(
                    "Dynamic string properties, single block=%s, multiple blocks=%s, could have been short (%d %%)",
                    singleBlock, multiBlock, percentageThatCouldHaveBeenShort );
        }

        private long allCount()
        {
            return singleBlock+multiBlock+shortCount();
        }

        private long shortCount()
        {
            long result = 0;
            for ( Statistics stats : statistics.values() )
            {
                result += stats.count();
            }
            return result;
        }
        
        @Override
        public String toString()
        {
            StringBuilder result = toStringBuilder();
            for ( Pair<Statistics, Collection<String>> stats : longStringByLengthStatistics.values() )
            {
                if ( stats.first().hasData() )
                {
                    appendStat( result, stats.first() );
//                    result.append( "\n  samples:" );
//                    for ( String sample : stats.other() )
//                    {
//                        result.append( "\n   " + sample );
//                    }
                }
            }
            return result.toString();
        }
    }

    final Map<ShortStringEncoding, Statistics> statistics = new EnumMap<ShortStringEncoding, Statistics>(
            ShortStringEncoding.class );

    private StringStatistics()
    {
        super( null, null );
        for ( ShortStringEncoding encoding : ShortStringEncoding.values() )
        {
            statistics.put( encoding, new Statistics( null, encoding.name() ) );
        }
    }

    @Override
    public String toString()
    {
        return toStringBuilder().toString();
    }

    protected StringBuilder toStringBuilder()
    {
        StringBuilder result = new StringBuilder( header() ).append( ":" );
        boolean none = true;
        for ( Statistics stats : statistics.values() )
        {
            if ( stats.hasData() )
            {
                appendStat( result, stats );
                none = false;
            }
        }
        if ( none ) result.append( " none" );
        return result;
    }

    protected static void appendStat( StringBuilder result, Statistics stats )
    {
        result.append( "\n    " ).append( stats );
    }

    abstract String header();

    @Override
    boolean hasData()
    {
        return true;
    }
}
