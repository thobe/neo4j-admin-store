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

import java.util.Iterator;

import org.neo4j.helpers.collection.PrefetchingIterator;

class RecordChain<R extends Abstract64BitRecord, S extends StoreAccess<?, R> & ChainStore<R>> implements
        Iterable<R>
{
    private final S store;
    private final long firstId;

    RecordChain( S store, long firstId )
    {
        this.store = store;
        this.firstId = firstId;
    }

    @Override
    public Iterator<R> iterator()
    {
        return new PrefetchingIterator<R>()
        {
            R current = firstId == PropertyStoreAccess.NO_NEXT_RECORD ? null : store.forceGetRecord( firstId );

            @Override
            protected R fetchNextOrNull()
            {
                try
                {
                    return current;
                }
                finally
                {
                    if ( current != null ) current = store.nextRecordInChainOrNull( current );
                }
            }
        };
    }
}
