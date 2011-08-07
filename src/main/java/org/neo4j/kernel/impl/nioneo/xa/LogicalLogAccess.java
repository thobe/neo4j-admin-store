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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;

import javax.transaction.xa.Xid;

import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry;
import org.neo4j.kernel.impl.transaction.xaframework.LogIoUtils;
import org.neo4j.kernel.impl.transaction.xaframework.ReadPastEndException;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommand;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommandFactory;

public class LogicalLogAccess
{
    private final ByteBuffer buffer;
    private final FileChannel log;
    private final long version, prevTx;
    private final XaCommandFactory commands = new XaCommandFactory()
    {
        @Override
        public XaCommand readCommand( ReadableByteChannel byteChannel, @SuppressWarnings( "hiding" ) ByteBuffer buffer )
                throws IOException
        {
            return Command.readCommand( null, byteChannel, buffer );
        }
    };
    private final String path;
    private final long start;

    LogicalLogAccess( String path, String name )
    {
        this( new File( path, name ) );
    }

    public LogicalLogAccess( String fileName )
    {
        this( new File( fileName ) );
    }

    private LogicalLogAccess( File file )
    {
        try
        {
            this.log = new RandomAccessFile( file, "r" ).getChannel();
        }
        catch ( FileNotFoundException e )
        {
            throw new IllegalArgumentException( "no such file: " + file, e );
        }
        this.buffer = ByteBuffer.allocateDirect( 9 + Xid.MAXGTRIDSIZE + Xid.MAXBQUALSIZE * 10 );
        long[] header;
        @SuppressWarnings( "hiding" ) long start = 0;
        try
        {
            header = LogIoUtils.readLogHeader( buffer, log, false );
            start = log.position();
        }
        catch ( IOException e )
        {
            header = null;
        }
        if ( header == null )
        {
            header = new long[] { -1, -1 };
        }
        this.start = start;
        this.version = header[0];
        this.prevTx = header[1];
        this.path = file.getPath();
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[path=" + path + "; version=" + version + "; prevTx=" + prevTx + "]";
    }

    public void reset() throws IOException
    {
        log.position( start );
    }

    public LogEntry next() throws IOException, ReadPastEndException
    {
        long pos = log.position();
        try
        {
            return LogIoUtils.readLogEntry( buffer, log, commands );
        }
        catch ( IOException e )
        {
            log.position( pos );
            throw e;
        }
    }

    public byte stepForward() throws IOException, ReadPastEndException
    {
        return LogIoUtils.readNextByte( buffer, log );
    }

    public void stepBackward() throws IOException
    {
        log.position( log.position() - 1 );
    }

    public void dump( PrintStream out )
    {
        for ( LogEntry entry : IteratorUtil.asIterable( new EntryIterator() ) )
        {
            out.println( entry );
        }
    }

    public boolean isHeaderOk()
    {
        return version != -1 && prevTx != -1;
    }

    public void close()
    {
        try
        {
            log.close();
        }
        catch ( IOException e )
        {
            e.printStackTrace();
        }
    }

    private class EntryIterator extends PrefetchingIterator<LogEntry>
    {
        @Override
        protected LogEntry fetchNextOrNull()
        {
            try
            {
                LogEntry entry;
                do
                {
                    entry = LogicalLogAccess.this.next();
                }
                while ( entry == null );
                return entry;
            }
            catch ( IOException e )
            {
                throw new IllegalStateException( e );
            }
            catch ( ReadPastEndException e )
            {
                return null;
            }
        }
    }
}
