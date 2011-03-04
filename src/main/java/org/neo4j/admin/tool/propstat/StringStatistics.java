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

import java.util.EnumMap;
import java.util.Map;

import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.ShortStringEncoding;
import org.neo4j.kernel.impl.nioneo.store.StringPropertyStoreAccess;

abstract class StringStatistics extends Statistics
{
    private static final int NO_NEXT_BLOCK = Record.NO_NEXT_BLOCK.intValue();

    static class ShortString extends StringStatistics
    {
        @Override
        void add( long payload )
        {
            statistics.get( ShortStringEncoding.getEncoding( payload ) ).add( 0 );
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

        DynamicString( StringPropertyStoreAccess stringStore )
        {
            this.stringStore = stringStore;
        }

        @Override
        void add( long payload )
        {
            DynamicRecord record = stringStore.forceGetRecord( (int) payload );
            if ( record.getNextBlock() == NO_NEXT_BLOCK )
            {
                ShortStringEncoding encoding = ShortStringEncoding.getEncoding( stringStore.toString( record ) );
                if ( encoding != null )
                {
                    statistics.get( encoding ).add( 0 );
                }
                else
                {
                    singleBlock++;
                }
            }
            else
            {
                multiBlock++;
            }
        }

        @Override
        @SuppressWarnings( "boxing" )
        String header()
        {
            return String.format(
                    "Dynamic string properties, single block=%s, multiple blocks=%s, could have been short",
                    singleBlock, multiBlock );
        }
    }

    final Map<ShortStringEncoding, Statistics> statistics = new EnumMap<ShortStringEncoding, Statistics>(
            ShortStringEncoding.class );

    private StringStatistics()
    {
        for ( ShortStringEncoding encoding : ShortStringEncoding.values() )
        {
            statistics.put( encoding, new Statistics.Simple( encoding.name() ) );
        }
    }

    @Override
    public String toString()
    {
        StringBuilder result = new StringBuilder( header() ).append( ":" );
        boolean none = true;
        for ( Statistics stats : statistics.values() )
        {
            if ( stats.hasData() )
            {
                result.append( "\n    " ).append( stats );
                none = false;
            }
        }
        if ( none ) result.append( " none" );
        return result.toString();
    }

    abstract String header();

    @Override
    boolean hasData()
    {
        return true;
    }
}
