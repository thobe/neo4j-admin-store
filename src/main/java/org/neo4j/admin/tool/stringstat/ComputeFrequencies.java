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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

class ComputeFrequencies extends Processor
{
    private static class Counter
    {
        long count = 0;

        void add()
        {
            count++;
        }

        @Override
        public String toString()
        {
            return Long.toString( count );
        }
    }

    private static class DefaultMap extends HashMap<Character, Counter>
    {
        private static final long serialVersionUID = 1L;

        @Override
        public Counter get( Object key )
        {
            Counter result = super.get( key );
            if ( key instanceof Character )
            {
                if ( result == null ) put( (Character) key, result = new Counter() );
            }
            return result;
        }
    }

    private final Map<Character, Counter> fourBit = new DefaultMap();
    private final Map<Character, Counter> fiveBit = new DefaultMap();
    private final Map<Character, Counter> sixBit = new DefaultMap();

    ComputeFrequencies( StringBuilder out )
    {
        super( out );
    }

    @Override
    public String toString()
    {
        return "Computing character frequencies";
    }

    @Override
    public void process( String string )
    {
        if ( string.length() <= 10 )
            frequencies( fourBit, string.toCharArray() );
        else if ( string.length() <= 12 )
            frequencies( fiveBit, string.toCharArray() );
        else if ( string.length() <= 15 )
            frequencies( sixBit, string.toCharArray() );
        else
            return;
    }

    private void frequencies( Map<Character, Counter> frequencies, char[] chars )
    {
        for ( char c : chars )
            frequencies.get( Character.valueOf( c ) ).add();
    }

    @Override
    public Processor next()
    {
        StringType[] builtin = StringType.loadAll();
        StringType[] assumptions = { //
                computeFrequency( 4, sort( fourBit.entrySet() ) ),//
                computeFrequency( 5, sort( fiveBit.entrySet() ) ),//
                computeFrequency( 6, sort( sixBit.entrySet() ) ), //
        };
        StringType[] types = new StringType[builtin.length + assumptions.length];
        System.arraycopy( builtin, 0, types, 0, builtin.length );
        System.arraycopy( assumptions, 0, types, builtin.length, assumptions.length );
        return new TryAssumptions( out, types );
    }

    private List<Map.Entry<Character, Counter>> sort( Collection<Map.Entry<Character, Counter>> entries )
    {
        List<Map.Entry<Character, Counter>> result = new ArrayList<Map.Entry<Character, Counter>>( entries );
        Collections.sort( result, new Comparator<Map.Entry<Character, Counter>>()
        {
            @Override
            public int compare( Entry<Character, Counter> o1, Entry<Character, Counter> o2 )
            {
                return Long.signum( o2.getValue().count - o1.getValue().count );
            }
        } );
        return result;
    }

    private StringType computeFrequency( int bits, List<Map.Entry<Character, Counter>> frequencies )
    {
        print( "= %s bit frequencies =", Integer.toString( bits ) );
        String format = String.format( "  %%s: %%%ss",
                Integer.toString( frequencies.get( 0 ).getValue().toString().length() ) );
        Iterator<Map.Entry<Character, Counter>> iter = frequencies.iterator();
        char[] encoding = new char[1 << bits];
        bits = 1 << ( bits + 1 );
        for ( int i = 0; i < bits && iter.hasNext(); i++ )
        {
            if ( i % 4 == 0 ) print( "\n" );
            Entry<Character, Counter> item = iter.next();
            print( format, item.getKey(), item.getValue() );
            if ( i < encoding.length ) encoding[i] = item.getKey().charValue();
        }
        print( "\n" );
        return new FrequencyBased( encoding );
    }
}
