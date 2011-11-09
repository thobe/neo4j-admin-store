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

import java.io.File;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.helpers.Triplet;
import org.neo4j.kernel.CommonFactories;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.nioneo.xa.LogicalLogStore;

public class GraphDatabaseStore extends LogicalLogStore
{
    static final long NO_NEXT_REL = Record.NO_NEXT_RELATIONSHIP.intValue(),
            NO_PREV_REL = Record.NO_PREV_RELATIONSHIP.intValue();
    static final long NO_NEXT_PROP = Record.NO_NEXT_PROPERTY.intValue(),
            NO_PREV_PROP = Record.NO_PREVIOUS_PROPERTY.intValue();
    static final long NO_NEXT_BLOCK = Record.NO_NEXT_BLOCK.intValue(), NO_PREV_BLOCK = Record.NO_PREV_BLOCK.intValue();

    private static final Field STRING_PROPERTY_STORE, ARRAY_PROPERTY_STORE;
    static
    {
        // FIXME: this should not use reflection,
        // refactor PropertyStore to allow access to these fields
        STRING_PROPERTY_STORE = field( PropertyStore.class, "stringPropertyStore" );
        ARRAY_PROPERTY_STORE = field( PropertyStore.class, "arrayPropertyStore" );
    }
    private final NodeStore nodeStore;
    private final RelationshipStore relStore;
    private final RelationshipTypeStore relTypeStore;
    private final PropertyStore propStore;
    private final DynamicStringStore stringStore;
    private final DynamicArrayStore arrayStore;

    public GraphDatabaseStore( String path )
    {
        this( path, defaultParams() );
    }

    public static Object get( Class<?> type, String name, Object obj )
    {
        return get( obj, field( type, name ) );
    }

    private static Field field( Class<?> type, String name )
    {
        Field field;
        try
        {
            field = type.getDeclaredField( name );
        }
        catch ( Exception cause )
        {
            throw new RuntimeException( cause );
        }
        field.setAccessible( true );
        return field;
    }

    private static Object get( Object obj, Field field )
    {
        try
        {
            return field.get( obj );
        }
        catch ( Exception cause )
        {
            throw new RuntimeException( cause );
        }
    }

    public GraphDatabaseStore( String path, Map<Object, Object> params )
    {
        super( path );
        params.put( FileSystemAbstraction.class, CommonFactories.defaultFileSystemAbstraction() );
        this.nodeStore = new NodeStore( path + "/neostore.nodestore.db", params );
        this.relStore = new RelationshipStore( path + "/neostore.relationshipstore.db", params );
        this.relTypeStore = new RelationshipTypeStore( path + "/neostore.relationshiptypestore.db", params,
                IdType.RELATIONSHIP_TYPE );
        if ( new File( path + "/neostore.propertystore.db" ).exists() )
        {
            this.propStore = new PropertyStore( path + "/neostore.propertystore.db", params );
            this.stringStore = (DynamicStringStore) get( propStore, STRING_PROPERTY_STORE );
            this.arrayStore = (DynamicArrayStore) get( propStore, ARRAY_PROPERTY_STORE );
        }
        else
        {
            this.propStore = null;
            this.stringStore = null;
            this.arrayStore = null;
        }
    }

    @Override
    public void initialize()
    {
        makeStoreOk();
    }

    public void makeStoreOk()
    {
        nodeStore.makeStoreOk();
        relStore.makeStoreOk();
        if ( propStore != null )
        {
            propStore.makeStoreOk();
            stringStore.makeStoreOk();
            arrayStore.makeStoreOk();
        }
    }

    @Override
    public void shutdown()
    {
        nodeStore.close();
        relStore.close();
        if ( propStore != null ) propStore.close();
    }

    private static Map<Object, Object> defaultParams()
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
        params.put( Config.REBUILD_IDGENERATORS_FAST, "true" );
        //
        params.put( IdGeneratorFactory.class, new CommonFactories.DefaultIdGeneratorFactory() );
        return params;
    }

    public NodeStoreAccess getNodeStore()
    {
        return new NodeStoreAccess( nodeStore );
    }

    public RelationshipStoreAccess getRelStore()
    {
        return new RelationshipStoreAccess( relStore );
    }

    public PropertyStoreAccess getPropStore()
    {
        return propStore == null ? null : new PropertyStoreAccess( propStore );
    }

    public StringPropertyStoreAccess getStringPropertyStore()
    {
        return stringStore == null ? null : new StringPropertyStoreAccess( stringStore );
    }

    public ArrayPropertyStoreAccess getArrayPropertyStore()
    {
        return arrayStore == null ? null : new ArrayPropertyStoreAccess( arrayStore );
    }

    public Triplet<PropertyStoreAccess, StringPropertyStoreAccess, ArrayPropertyStoreAccess> newPropertyStore(
            String path )
    {
        return newPropertyStore( path, defaultParams() );
    }

    public Triplet<PropertyStoreAccess, StringPropertyStoreAccess, ArrayPropertyStoreAccess> newPropertyStore(
            String path, Map<Object, Object> params )
    {
        params.put( FileSystemAbstraction.class, CommonFactories.defaultFileSystemAbstraction() );
        String fileName = path + "/neostore.propertystore.db";
        PropertyStore.createStore( fileName, params );
        PropertyStore props = new PropertyStore( fileName, params );
        DynamicStringStore strings = (DynamicStringStore) get( propStore, STRING_PROPERTY_STORE );
        DynamicArrayStore arrays = (DynamicArrayStore) get( propStore, ARRAY_PROPERTY_STORE );
        return Triplet.of( new PropertyStoreAccess( props ), new StringPropertyStoreAccess( strings ),
                new ArrayPropertyStoreAccess( arrays ) );
    }

    public RelationshipTypeStoreAccess getTypeStore()
    {
        return new RelationshipTypeStoreAccess( relTypeStore );
    }

    public IndexKeyStoreAccess getIndexKeyStore()
    {
        return propStore == null ? null : new IndexKeyStoreAccess( propStore.getIndexStore() );
    }
}
