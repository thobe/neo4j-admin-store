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
package org.neo4j.kernel.impl.nioneo.store;

public class NodeStoreAccess
{
    private final NodeStore store;

    NodeStoreAccess( NodeStore store )
    {
        this.store = store;
    }

    public long getHighId()
    {
        return store.getHighId();
    }

    public NodeRecord getRecord( int id )
    {
        return store.getRecord( id );
    }

    public NodeRecord forceGetRecord( int id )
    {
        PersistenceWindow window = store.acquireWindow( id, OperationType.READ );
        try
        {
            Buffer buffer = window.getOffsettedBuffer( id );
            boolean inUse = ( buffer.get() == Record.IN_USE.byteValue() );
            NodeRecord nodeRecord = new NodeRecord( id );
            nodeRecord.setInUse( inUse );
            nodeRecord.setNextRel( buffer.getInt() );
            nodeRecord.setNextProp( buffer.getInt() );
            return nodeRecord;
        }
        finally
        {
            store.releaseWindow( window );
        }
    }

    public void forceUpdateRecord( NodeRecord record )
    {
        PersistenceWindow window = store.acquireWindow( record.getId(), OperationType.WRITE );
        try
        {
            int id = record.getId();
            Buffer buffer = window.getOffsettedBuffer( id );
            buffer.put( record.inUse() ? Record.IN_USE.byteValue() : Record.NOT_IN_USE.byteValue() ).putInt(
                    record.getNextRel() ).putInt( record.getNextProp() );
        }
        finally
        {
            store.releaseWindow( window );
        }
    }
}
