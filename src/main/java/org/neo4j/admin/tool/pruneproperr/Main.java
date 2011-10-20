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
package org.neo4j.admin.tool.pruneproperr;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.admin.tool.RecordProcessor;
import org.neo4j.admin.tool.SimpleStoreTool;
import org.neo4j.kernel.impl.nioneo.store.Abstract64BitRecord;
import org.neo4j.kernel.impl.nioneo.store.Filter;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeStoreAccess;
import org.neo4j.kernel.impl.nioneo.store.PropertyContainerStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyStoreAccess;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipStoreAccess;
import org.neo4j.kernel.impl.nioneo.store.StoreAccess;

public class Main extends SimpleStoreTool
{
    private final PropertyStoreAccess props;

    Main( String[] args )
    {
        super( args );
        props = store.getPropStore();
    }

    public static void main( String[] args ) throws Throwable
    {
        main( Main.class, args );
    }

    @SuppressWarnings( "unchecked" )
    @Override
    protected void run() throws Throwable
    {
        Map<Long, UsedProperty> properties = new HashMap<Long, UsedProperty>();
        NodeStoreAccess nodes = store.getNodeStore();
        RelationshipStoreAccess rels = store.getRelStore();
        process( new Processor<NodeRecord, NodeStoreAccess>( nodes, properties ), nodes, Filter.IN_USE );
        process( new Processor<RelationshipRecord, RelationshipStoreAccess>( rels, properties ), rels,
                Filter.IN_USE );
    }

    private class Processor<R extends Abstract64BitRecord, S extends StoreAccess<?, R> & PropertyContainerStore<R>>
            implements RecordProcessor<R>
    {
        @SuppressWarnings( "hiding" )
        private final S store;
        private final Map<Long, UsedProperty> properties;

        Processor( S store, Map<Long, UsedProperty> properties )
        {
            this.store = store;
            this.properties = properties;
        }

        @Override
        public void process( R record )
        {
            long propId = store.getFirstPropertyOf( record );
            if ( propId == PropertyStoreAccess.NO_NEXT_RECORD ) return;
            PropertyRecord propRec = props.forceGetRecord( propId );
            if ( !propRec.inUse() || propRec.getPrevProp() != PropertyStoreAccess.NO_PREV_RECORD )
            {// references wrong property (not in use || not first in chain)
                fix( record );
                return;
            }
            UsedProperty oldProp, newProp;
            oldProp = properties.put( Long.valueOf( propId ), newProp = new UsedProperty( record.getId() ) );
            if ( oldProp != null )
            {// multiple references to same property
                oldProp.fix( this );
                fix( record );
                newProp.id = -1;
            }
        }

        private void fix( R record )
        {
            System.out.println( record );
            store.setFirstPropertyOf( record, PropertyStoreAccess.NO_NEXT_RECORD );
            store.forceUpdateRecord( record );
        }

        void fix( long id )
        {
            fix( store.forceGetRecord( id ) );
        }
    }

    private static class UsedProperty
    {
        long id;

        public UsedProperty( long id )
        {
            this.id = id;
        }

        <R extends Abstract64BitRecord, S extends StoreAccess<?, R> & PropertyContainerStore<R>> void fix(
                Processor<R, S> processor )
        {
            if ( id != -1 ) processor.fix( id );
            id = -1;
        }
    }
}
