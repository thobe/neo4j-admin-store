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
package org.neo4j.kernel.impl.nioneo.xa;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class LogicalLogStore
{
    private final String path;

    public LogicalLogStore( String path )
    {
        this.path = path;
    }

    public void initialize()
    {
        // NOOP
    }

    public void shutdown()
    {
        // NOOP
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[" + path + "]";
    }

    public LogicalLogAccess activeLogicalLog()
    {
        // FIXME this is a bit of copy/paste coding here...
        String base = "nioneo_logical.log";
        File activeFile = new File( path, base + ".active" );
        if ( activeFile.exists() )
        {
            try
            {
                FileChannel fc = new RandomAccessFile( activeFile, "r" ).getChannel();
                byte bytes[] = new byte[256];
                ByteBuffer buf = ByteBuffer.wrap( bytes );
                int read = fc.read( buf );
                fc.close();
                if ( read != 4 )
                {
                    throw new IllegalStateException( "Read " + read + " bytes from " + activeFile.getName()
                                                     + " but expected 4" );
                }
                buf.flip();
                char c = buf.asCharBuffer().get();
                switch ( c )
                {
                case 'C':
                    return null;
                case '1':
                    return logicalLog( base + ".1" );
                case '2':
                    return logicalLog( base + ".2" );
                default:
                    throw new IllegalStateException( "Invalid actove log value: " + c );
                }
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }
        else
        {
            if ( new File( path, base + ".1" ).exists() )
            {
                return logicalLog( base + ".1" );
            }
            return null;
        }
    }

    public LogicalLogAccess logicalLog( String name )
    {
        return new LogicalLogAccess( path, name );
    }
}
