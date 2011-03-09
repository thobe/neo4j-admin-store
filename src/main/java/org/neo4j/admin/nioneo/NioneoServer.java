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
package org.neo4j.admin.nioneo;

import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.Map;
import org.neo4j.kernel.impl.nioneo.store.Abstract64BitRecord;

import org.neo4j.kernel.impl.nioneo.store.DynamicStoreAccess;
import org.neo4j.kernel.impl.nioneo.store.GraphDatabaseStore;
import org.neo4j.kernel.impl.nioneo.store.NodeStoreAccess;
import org.neo4j.kernel.impl.nioneo.store.PropertyStoreAccess;
import org.neo4j.kernel.impl.nioneo.store.RelationshipStoreAccess;
import org.neo4j.kernel.impl.nioneo.store.StringPropertyStoreAccess;
import org.neo4j.shell.SimpleAppServer;

public class NioneoServer extends SimpleAppServer
{
    private Abstract64BitRecord currentRecord;
    private final GraphDatabaseStore store;

    public NioneoServer( String path ) throws RemoteException
    {
        super();
        store = new GraphDatabaseStore( path );
        store.makeStoreOk();
    }

    private Map<Object, Object> getDefaultParams()
    {
        Map<Object, Object> params = new HashMap<Object, Object>();
        params.put( "neostore.nodestore.db.mapped_memory", "20M" );
        params.put( "neostore.propertystore.db.mapped_memory", "90M" );
        params.put( "neostore.propertystore.db.index.mapped_memory", "1M" );
        params.put( "neostore.propertystore.db.index.keys.mapped_memory", "1M" );
        params.put( "neostore.propertystore.db.strings.mapped_memory", "130M" );
        params.put( "neostore.propertystore.db.arrays.mapped_memory", "130M" );
        params.put( "neostore.relationshipstore.db.mapped_memory", "100M" );
        // if on windows, default no memory mapping
        String nameOs = System.getProperty( "os.name" );
        if ( nameOs.startsWith( "Windows" ) )
        {
            params.put( "use_memory_mapped_buffers", "false" );
        }
        return params;
    }

    public NodeStoreAccess getNodeStore()
    {
        return store.getNodeStore();
    }

    public RelationshipStoreAccess getRelStore()
    {
        return store.getRelStore();
    }

    public PropertyStoreAccess getPropStore()
    {
        return store.getPropStore();
    }

    public StringPropertyStoreAccess getStringStore()
    {
        return store.getStringPropertyStore();
    }

    public DynamicStoreAccess getArrayStore()
    {
        return store.getArrayPropertyStore();
    }

    @Override
    public void shutdown()
    {
        super.shutdown();
        store.shutdown();
    }

    public void setRecord( Abstract64BitRecord record )
    {
        this.currentRecord = record;
    }

    public Abstract64BitRecord getCurrentRecord()
    {
        return this.currentRecord;
    }
}
