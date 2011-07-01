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
package org.neo4j.admin.tool.tmdumplog;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Collection;
import java.util.TreeSet;

import org.neo4j.kernel.impl.transaction.TxLogAccess;

public class Main
{
    public static void main( String[] args )
    {
        for ( String arg : args )
        {
            for ( String fileName : filenamesOf( arg ) )
            {
                TxLogAccess log;
                try
                {
                    log = new TxLogAccess( fileName );
                }
                catch ( IOException e )
                {
                    System.err.println( "Could not open: " + fileName );
                    e.printStackTrace( System.err );
                    continue;
                }
                System.out.println( "\n=== " + log + " ===" );
                log.dump( System.out );
                log.close();
            }
        }
    }

    private static String[] filenamesOf( String string )
    {
        File file = new File( string );
        if ( file.isDirectory() )
        {
            File[] files = file.listFiles( new FilenameFilter()
            {
                public boolean accept( File dir, String name )
                {
                    return name.startsWith( "tm_tx_log" );
                }
            } );
            Collection<String> result = new TreeSet<String>();
            for ( int i = 0; i < files.length; i++ )
            {
                result.add( files[i].getPath() );
            }
            return result.toArray( new String[result.size()] );
        }
        else
        {
            return new String[] { string };
        }
    }
}
