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

import org.neo4j.admin.nioneo.store.AbstractRecord;
import org.neo4j.admin.nioneo.store.DynamicArrayStore;
import org.neo4j.admin.nioneo.store.DynamicStringStore;
import org.neo4j.admin.nioneo.store.NodeStore;
import org.neo4j.admin.nioneo.store.PropertyStore;
import org.neo4j.admin.nioneo.store.RelationshipStore;
import org.neo4j.shell.SimpleAppServer;

public class NioneoServer extends SimpleAppServer
{
    private final NodeStore nodeStore;
    private final RelationshipStore relStore;
    private final PropertyStore propStore;
    private final DynamicStringStore stringStore;
    private final DynamicArrayStore arrayStore;
    
    private AbstractRecord currentRecord;

    public NioneoServer( String path ) throws RemoteException
    {
        super();
        nodeStore = new NodeStore( path + "/neostore.nodestore.db",
                getDefaultParams() );
        relStore = new RelationshipStore( path
                                          + "/neostore.relationshipstore.db",
                getDefaultParams() );
        propStore = new PropertyStore( path + "/neostore.propertystore.db",
                getDefaultParams() );
        stringStore = propStore.getStringStore();
        arrayStore = propStore.getArrayStore();
        nodeStore.makeStoreOk();
        relStore.makeStoreOk();
        propStore.makeStoreOk();
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

    public NodeStore getNodeStore()
    {
        return nodeStore;
    }

    public RelationshipStore getRelStore()
    {
        return relStore;
    }

    public PropertyStore getPropStore()
    {
        return propStore;
    }

    public DynamicStringStore getStringStore()
    {
        return stringStore;
    }

    public DynamicArrayStore getArrayStore()
    {
        return arrayStore;
    }
    
    public void shutdown()
    {
        super.shutdown();
        nodeStore.close();
        relStore.close();
        propStore.close();
    }
    
    public void setRecord( AbstractRecord record )
    {
        this.currentRecord = record;
    }
    
    public AbstractRecord getCurrentRecord()
    {
        return this.currentRecord;
    }
}
