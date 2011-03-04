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
package org.neo4j.admin.tool.stringstat;

import org.neo4j.kernel.impl.nioneo.store.StringPropertyStoreAccess;

class TryAssumptions extends Processor
{
    private final int[] stats;
    private final StringType[] types;

    TryAssumptions( StringPropertyStoreAccess strings, StringBuilder out, StringType[] types )
    {
        super( strings, out );
        this.stats = new int[1 << types.length];
        this.types = types;
    }

    @Override
    public String toString()
    {
        return "Matching potential encodings";
    }

    @Override
    public void process( String string )
    {
        int category = 0;
        for ( int i = 0; i < types.length; i++ )
        {
            category |= ( types[i].matches( string ) ? 1 : 0 ) << i;
        }
        stats[category]++;
    }

    @Override
    public Processor next()
    {
        for ( int i = 0; i < stats.length; i++ )
        {
            if ( stats[i] != 0 )
                print( "%10s strings with category bitmask %16s%n", Integer.toString( stats[i] ),
                        "0b" + Integer.toBinaryString( i ) );
        }
        print( "= Category index =\n" );
        for ( int i = 0; i < types.length; i++ )
        {
            print( "%2s. %s%n", Integer.toString( i ), types[i] );
        }
        return null;
    }
}
