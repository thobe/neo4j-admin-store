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
package org.neo4j.kernel.impl.transaction;

import java.io.IOException;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.Iterator;

import javax.transaction.xa.Xid;

import org.neo4j.kernel.impl.transaction.TxLog.Record;

public class TxLogAccess
{
    private final String name;
    private final FileChannel fileChannel;

    public TxLogAccess( String fileName ) throws IOException
    {
        if ( fileName == null )
        {
            throw new IllegalArgumentException( "Null filename" );
        }
        fileChannel = new RandomAccessFile( fileName, "rw" ).getChannel();
        this.name = fileName;
    }

    @Override
    public String toString()
    {
        return "TxLog[" + name + "]";
    }

    public void dump( PrintStream out )
    {
        for ( TxLog.Record record : records() )
        {
            out.println( record );
        }
    }

    private Iterable<TxLog.Record> records()
    {
        return new Iterable<TxLog.Record>()
        {
            @Override
            public Iterator<TxLog.Record> iterator()
            {
                try
                {
                    final ByteBuffer buffer = ByteBuffer
                            .allocateDirect( ( 3 + Xid.MAXGTRIDSIZE + Xid.MAXBQUALSIZE ) * 1000 );
                    fileChannel.position( 0 );
                    buffer.clear();
                    fileChannel.read( buffer );
                    buffer.flip();

                    return new Iterator<TxLog.Record>()
                    {
                        private int seqNr = 0;
                        private long nextPosition = 0;

                        @Override
                        public boolean hasNext()
                        {
                            return buffer.hasRemaining();
                        }

                        // TODO: this code could make it back into TxLog, it is
                        // a better version of the code in getDanglingRecords
                        @Override
                        public Record next()
                        {
                            byte recordType = buffer.get();

                            byte globalId[] = new byte[buffer.get()];
                            byte branchId[] = new byte[recordType == TxLog.BRANCH_ADD ? buffer.get() : 0];

                            if ( buffer.limit() - buffer.position() < globalId.length + branchId.length )
                            {
                                reRead( buffer );
                                return next();
                            }

                            buffer.get( globalId );
                            buffer.get( branchId );

                            try
                            {
                                return new Record( recordType, globalId, branchId, seqNr++ );
                            }
                            finally
                            {
                                nextPosition += ( recordType == TxLog.BRANCH_ADD ? 3 : 2 ) + globalId.length
                                                + branchId.length;
                                if ( !buffer.hasRemaining() ) reRead( buffer );
                            }
                        }

                        private void reRead( final ByteBuffer buffer )
                        {
                            buffer.clear();
                            try
                            {
                                fileChannel.position( nextPosition );
                                fileChannel.read( buffer );
                            }
                            catch ( IOException e )
                            {
                                // ignore
                            }
                            buffer.flip();
                        }

                        @Override
                        public void remove()
                        {
                            throw new UnsupportedOperationException();
                        }
                    };
                }
                catch ( IOException e )
                {
                    return Collections.<TxLog.Record>emptySet().iterator();
                }
            }
        };
    }

    public void close()
    {
        try
        {
            fileChannel.close();
        }
        catch ( IOException e )
        {
            // ignore
        }
    }
}
