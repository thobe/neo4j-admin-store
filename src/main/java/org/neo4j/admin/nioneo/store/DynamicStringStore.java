/*
 * Copyright (c) 2002-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.admin.nioneo.store;

import java.util.Map;

/**
 * Dynamic store that stores strings.
 */
public class DynamicStringStore extends AbstractDynamicStore
{
    // store version, each store ends with this string (byte encoded)
    private static final String VERSION = "StringPropertyStore v0.9.5";

    public DynamicStringStore( String fileName, Map<?,?> config )
    {
        super( fileName, config );
    }

    public DynamicStringStore( String fileName )
    {
        super( fileName );
    }

    public String getTypeAndVersionDescriptor()
    {
        return VERSION;
    }

    public static void createStore( String fileName, int blockSize )
    {
        createEmptyStore( fileName, blockSize, VERSION );
    }
    
    public void setHighId( long highId )
    {
        super.setHighId( highId );
    }
    
    public int nextBlockId()
    {
        return super.nextBlockId();
    }

    @Override
    protected boolean versionFound( String version )
    {
        if ( !version.startsWith( "StringPropertyStore" ) )
        {
            // non clean shutdown, need to do recover with right neo
            return false;
        }
        if ( version.equals( "StringPropertyStore v0.9.3" ) )
        {
            rebuildIdGenerator();
            closeIdGenerator();
            return true;
        }
        throw new IllegalStoreVersionException( "Store version [" + version  + 
            "]. Please make sure you are not running old Neo4j kernel " + 
            " towards a store that has been created by newer version " + 
            " of Neo4j." );
    }
}