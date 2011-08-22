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

import java.nio.ByteBuffer;

public class StringPropertyStoreAccess extends DynamicStoreAccess<DynamicStringStore>
{
    StringPropertyStoreAccess( DynamicStringStore store )
    {
        super( store );
    }

    @Override
    public DynamicRecord forceGetRecord( long blockId )
    {
        DynamicRecord record = super.forceGetRecord( blockId );
        record.setType( PropertyType.STRING.intValue() );
        return record;
    }

    public String toString( DynamicRecord record )
    {
        makeHeavy( record );
        final char[] chars;
        if ( !record.isCharData() )
        {
            ByteBuffer buf = ByteBuffer.wrap( record.getData() );
            chars = new char[record.getData().length / 2];
            buf.asCharBuffer().get( chars );
        }
        else
        {
            chars = record.getDataAsChar();
        }
        return new String( chars, 0, record.getLength() / 2 );
    }
}
