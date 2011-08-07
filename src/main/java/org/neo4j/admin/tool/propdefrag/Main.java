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
package org.neo4j.admin.tool.propdefrag;

import java.io.File;
import java.io.IOException;

import org.neo4j.admin.tool.SimpleStoreTool;
import org.neo4j.helpers.Triplet;
import org.neo4j.kernel.impl.nioneo.store.Abstract64BitRecord;
import org.neo4j.kernel.impl.nioneo.store.ArrayPropertyStoreAccess;
import org.neo4j.kernel.impl.nioneo.store.ChainStore;
import org.neo4j.kernel.impl.nioneo.store.Filter;
import org.neo4j.kernel.impl.nioneo.store.PropertyContainerStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyStoreAccess;
import org.neo4j.kernel.impl.nioneo.store.StoreAccess;
import org.neo4j.kernel.impl.nioneo.store.StringPropertyStoreAccess;

// just drafted - not tested!
public class Main extends SimpleStoreTool
{
    public static void main( String[] args ) throws Throwable
    {
        main( Main.class, args );
    }

    private final PropertyStoreAccess newProps;
    private final StringPropertyStoreAccess newStrings;
    private final ArrayPropertyStoreAccess newArrays;
    private final File path;

    Main( String[] args ) throws IOException
    {
        super( args );
        path = File.createTempFile( "neo4j", "propdefrag" );
        if ( !path.mkdir() ) throw new IOException( "could not create temporary directory: " + path );
        Triplet<PropertyStoreAccess, StringPropertyStoreAccess, ArrayPropertyStoreAccess> stores = store
                .newPropertyStore( path.getAbsolutePath() );
        this.newProps = stores.first();
        this.newStrings = stores.second();
        this.newArrays = stores.third();
    }

    @Override
    protected void run() throws Throwable
    {
        rebuild( store.getNodeStore() );
        rebuild( store.getRelStore() );
    }

    @SuppressWarnings( "unchecked" )
    private <R extends Abstract64BitRecord, S extends StoreAccess<?, R> & PropertyContainerStore<R>> void rebuild(
            S entities )
    {
        PropertyStoreAccess props = store.getPropStore();
        final StringPropertyStoreAccess strings = store.getStringPropertyStore();
        final ArrayPropertyStoreAccess arrays = store.getArrayPropertyStore();
        for ( R entity : entities.scan( Filter.IN_USE ) )
        {
            entities.setFirstPropertyOf(
                    entity,
                    rebuildChain( props, newProps, entities.getFirstPropertyOf( entity ),
                            new Callback<PropertyRecord>()
                            {
                                @Override
                                @SuppressWarnings( "incomplete-switch" )
                                void call( PropertyRecord record )
                                {
                                    switch ( record.getType() )
                                    {
                                    case STRING:
                                        rebuildChain( strings, newStrings, record.getPropBlock(), null );
                                        break;
                                    case ARRAY:
                                        rebuildChain( arrays, newArrays, record.getPropBlock(), null );
                                        break;
                                    }
                                }
                            } ) );
            entities.forceUpdateRecord( entity );
        }
    }

    private static abstract class Callback<R extends Abstract64BitRecord>
    {
        abstract void call( R record );
    }

    private static <R extends Abstract64BitRecord, S extends StoreAccess<?, R> & ChainStore<R>> long rebuildChain(
            S sources, S targets, long firstId, Callback<R> callback )
    {
        long first = PropertyStoreAccess.NO_NEXT_RECORD;
        R prev = null;
        for ( R source : sources.chain( firstId ) )
        {
            R target = targets.copy( source, targets.nextId() );
            if ( callback != null ) callback.call( target );
            if ( prev == null )
            {
                first = target.getId();
            }
            else
            {
                targets.linkChain( prev, target );
                targets.forceUpdateRecord( prev );
            }
            prev = target;
        }
        targets.forceUpdateRecord( prev );
        return first;
    }

    @Override
    protected void shutdown()
    {
        File target = new File( store.getPath() );
        newProps.close();
        super.shutdown();
        // overwrite the old store files with the new ones
        for ( File file : path.listFiles() )
        {
            File dest = new File( target, file.getName() );
            if ( !file.renameTo( dest ) )
            {
                if ( !( dest.exists() && dest.delete() && file.renameTo( dest ) ) )
                {
                    throw new RuntimeException( "Could not overwrite " + file.getName() );
                }
            }
        }
    }
}
