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

import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyStoreAccess;

class Statistics
{
    private final PropertyStoreAccess propertyStore;
    private final String name;
    private long count;
    
    Statistics( PropertyStoreAccess propertyStore, String name )
    {
        this.propertyStore = propertyStore;
        this.name = name;
    }
    
    protected PropertyStoreAccess getPropertyStore()
    {
        return propertyStore;
    }

    void add( Object value, PropertyRecord record )
    {
        incrementCount();
    }
    
    Object extractValue( PropertyRecord record )
    {
        return record.getType().getValue( record, getPropertyStore().getStore() );
    }

    @Override
    public String toString()
    {
        return name + ": " + count();
    }

    boolean hasData()
    {
        return count > 0;
    }
    
    protected void incrementCount()
    {
        count++;
    }
    
    long count()
    {
        return count;
    }
}
