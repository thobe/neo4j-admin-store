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

import static org.junit.Assert.assertEquals;
import static org.neo4j.admin.tool.propstat.ArrayStatistics.lengthAndBitDepth;

import org.junit.Test;

public class ArrayStatisticsTest
{
    @Test
    public void shouldGroupArraysByType() throws Exception
    {
        ArrayStatistics stats = new ArrayStatistics( null, null );

        stats.add( new boolean[0], null );

        assertEquals( 1, stats.getTotalCount() );
        assertEquals( 1, stats.getTypes().size() );
        assertEquals( "boolean", stats.getTypes().get( 0 ) );
    }

    @Test
    public void forEachTypeShouldGroupByLength() throws Exception
    {
        ArrayStatistics stats = new ArrayStatistics( null, null );

        stats.add( new boolean[0], null );
        stats.add( new boolean[0], null );
        stats.add( new boolean[0], null );
        stats.add( new boolean[1], null );
        stats.add( new boolean[1], null );
        stats.add( new boolean[2], null );

        assertEquals( 3, stats.lengthFrequenciesForType( "boolean" ).frequencyOf( 0 ) );
        assertEquals( 2, stats.lengthFrequenciesForType( "boolean" ).frequencyOf( 1 ) );
        assertEquals( 1, stats.lengthFrequenciesForType( "boolean" ).frequencyOf( 2 ) );

        assertEquals( "boolean\t0\t3\n" + "boolean\t1\t2\n" + "boolean\t2\t1\n", stats
                .lengthFrequenciesForType( "boolean" ).toString( "boolean" ) );
    }
    
    @Test
    public void shouldCalculateBitDepth() throws Exception
    {
        assertEquals( 0, ArrayStatistics.calculateBitDepth( new int[] {0} ) );
        assertEquals( 1, ArrayStatistics.calculateBitDepth( new int[] {1} ) );
        assertEquals( 3, ArrayStatistics.calculateBitDepth( new int[] {7} ) );
        assertEquals( 4, ArrayStatistics.calculateBitDepth( new int[] {8} ) );
        
        assertEquals( 4, ArrayStatistics.calculateBitDepth( new int[] { 2, 8, 3, 4, 5 } ) );
        assertEquals( 5, ArrayStatistics.calculateBitDepth( new int[] { 2, 8, 3, 4, 5, 17 } ) );
    }
    
    @Test
    public void shouldGroupByBitDepth() throws Exception
    {
        ArrayStatistics stats = new ArrayStatistics( null, null );

        stats.add( new int[] {0, 1, 0, 0}, null );
        stats.add( new int[] {0, 1, 0, 1}, null );
        stats.add( new int[] {1, 1, 0, 1}, null );
        
        stats.add( new int[] {1, 3, 0, 1}, null );
        stats.add( new int[] {1, 3, 0, 2}, null );
        
        stats.add( new int[] {1, 3, 0, 7}, null );
        
        System.out.println( stats.bitDepthFrequenciesForType( "int" ).toString( "int" ) );
       
        assertEquals( 3, stats.bitDepthFrequenciesForType( "int" ).frequencyOf( 1 ) );
        assertEquals( 2, stats.bitDepthFrequenciesForType( "int" ).frequencyOf( 2 ) );
        assertEquals( 1, stats.bitDepthFrequenciesForType( "int" ).frequencyOf( 3 ) );
    }
    
    @Test
    public void shouldGroupByLengthAndBitDepth() throws Exception
    {
        ArrayStatistics stats = new ArrayStatistics( null, null );

        stats.add( new int[] {0, 1, 0, 0}, null );
        stats.add( new int[] {0, 1, 0, 1}, null );
        stats.add( new int[] {1, 1, 0, 1}, null );
        
        stats.add( new int[] {1, 3, 0}, null );
        stats.add( new int[] {1, 3, 0}, null );
        
        stats.add( new int[] {7}, null );
        
        assertEquals( 3, stats.lengthAndBitDepthFrequenciesForType( "int" ).frequencyOf( lengthAndBitDepth( 4, 1) ) );
        assertEquals( 2, stats.lengthAndBitDepthFrequenciesForType( "int" ).frequencyOf( lengthAndBitDepth(3, 2) ) );
        assertEquals( 1, stats.lengthAndBitDepthFrequenciesForType( "int" ).frequencyOf( lengthAndBitDepth(1, 3) ) );
        
        System.out.println( stats );
    }

}
