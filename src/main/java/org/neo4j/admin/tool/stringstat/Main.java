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

import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.Filter;
import org.neo4j.kernel.impl.nioneo.store.GraphDatabaseStore;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.StringPropertyStoreAccess;

class Main implements Filter<DynamicRecord>
{
    @SuppressWarnings( "unchecked" )
    public static void main( String[] args )
    {
        if ( args.length != 1 )
        {
            System.out.println( "USAGE: java -jar " + jar( Main.class ) + " <path to neo4j store dir>" );
            return;
        }
        StringBuilder report = new StringBuilder( "<STRING STORE STATISTICS>\n" );
        Processor step = new ComputeFrequencies( report );
        Filter<DynamicRecord> filter = new Main();

        GraphDatabaseStore store = new GraphDatabaseStore( args[0] );
        store.makeStoreOk();
        try
        {
            StringPropertyStoreAccess strings = store.getStringPropertyStore();
            long highId = strings.getHighId();
            while ( step != null )
            {
                System.out.printf( "%s for %s string records%n", step, Long.toString( highId ) );
                int lastPercent = 0;
                for ( DynamicRecord record : strings.scan( filter ) )
                {
                    step.process( strings.toString( record ) );
                    int permille = (int) ( ( record.getId() * 1000L ) / highId );
                    if ( permille != lastPercent ) progress( lastPercent = permille );
                }
                if ( lastPercent != 1000 ) progress( 1000 );
                step = step.next();
            }
        }
        finally
        {
            store.shutdown();
        }
        System.out.print( report.append( "</STRING STORE STATISTICS>\n" ).toString() );
        System.out.flush();
    }

    private static void progress( int permille )
    {
        if ( permille % 100 == 0 )
            System.out.printf( "%3s%%%n", Integer.toString( permille / 10 ) );
        else if ( permille % 5 == 0 )
            System.out.print( "." );
    }

    private static final int NO_NEXT_BLOCK = Record.NO_NEXT_BLOCK.intValue();

    public boolean accept( DynamicRecord record )
    {
        return record.inUse() && record.getNextBlock() == NO_NEXT_BLOCK && record.getNextBlock() == NO_NEXT_BLOCK;
    }

    private static String jar( Class<?> type )
    {
        return type.getProtectionDomain().getCodeSource().getLocation().getFile();
    }
}
