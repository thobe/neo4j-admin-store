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
package org.neo4j.admin.check;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.neo4j.admin.tool.RecordProcessor;
import org.neo4j.admin.tool.StoreToolRunner;
import org.neo4j.kernel.impl.nioneo.store.Abstract64BitRecord;
import org.neo4j.kernel.impl.nioneo.store.ArrayPropertyStoreAccess;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.DynamicStoreAccess;
import org.neo4j.kernel.impl.nioneo.store.Filter;
import org.neo4j.kernel.impl.nioneo.store.GraphDatabaseStore;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeStoreAccess;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyStoreAccess;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipStoreAccess;
import org.neo4j.kernel.impl.nioneo.store.StringPropertyStoreAccess;

@SuppressWarnings( "unchecked" )
public class RecordInconsistency
{
    public static List<RecordInconsistency> check( GraphDatabaseStore store )
    {
        List<RecordInconsistency> result = new ArrayList<RecordInconsistency>();
        checkNodeStore( store.getNodeStore(), store.getRelStore(), store.getPropStore(), result );
        checkRelStore( store.getRelStore(), store.getNodeStore(), store.getPropStore(), result );
        checkPropStore( store.getPropStore(), store.getStringPropertyStore(), store.getArrayPropertyStore(), result );
        return result;
    }

    private static void checkNodeStore( NodeStoreAccess nodeStore, RelationshipStoreAccess relStore,
            PropertyStoreAccess propStore, Collection<RecordInconsistency> result )
    {
        for ( NodeRecord node : nodeStore.scan( Filter.IN_USE ) )
        {
            checkNode( relStore, propStore, result, node );
        }
    }

    private static void checkRelStore( RelationshipStoreAccess relStore, NodeStoreAccess nodeStore,
            PropertyStoreAccess propStore, Collection<RecordInconsistency> result )
    {
        for ( RelationshipRecord rel : relStore.scan( Filter.IN_USE ) )
        {
            checkRel( relStore, nodeStore, propStore, result, rel );
        }
    }

    private static void checkPropStore( PropertyStoreAccess propStore, StringPropertyStoreAccess stringStore,
            ArrayPropertyStoreAccess arrayStore, Collection<RecordInconsistency> result )
    {
        if ( propStore == null ) return;
        for ( PropertyRecord prop : propStore.scan( Filter.IN_USE ) )
        {
            checkProp( propStore, stringStore, arrayStore, result, prop );
        }
        checkDynamicStore( stringStore, result );
        checkDynamicStore( arrayStore, result );
    }

    private static void checkDynamicStore( DynamicStoreAccess<?> store, Collection<RecordInconsistency> result )
    {
        for ( DynamicRecord record : store.scan( Filter.IN_USE ) )
        {
            checkDynamic( store, result, record );
        }
    }

    public static void check( StoreToolRunner runner, final Collection<RecordInconsistency> result )
    {
        final NodeStoreAccess nodeStore = runner.store().getNodeStore();
        final RelationshipStoreAccess relStore = runner.store().getRelStore();
        final PropertyStoreAccess propStore = runner.store().getPropStore();
        final StringPropertyStoreAccess stringStore = runner.store().getStringPropertyStore();
        final ArrayPropertyStoreAccess arrayStore = runner.store().getArrayPropertyStore();
        runner.process( new RecordProcessor<NodeRecord>()
        {
            @Override
            public void process( NodeRecord record )
            {
                checkNode( relStore, propStore, result, record );
            }
        }, nodeStore, Filter.IN_USE );
        runner.process( new RecordProcessor<RelationshipRecord>()
        {
            @Override
            public void process( RelationshipRecord record )
            {
                checkRel( relStore, nodeStore, propStore, result, record );
            }
        }, relStore, Filter.IN_USE );
        if ( propStore != null )
        {
            runner.process( new RecordProcessor<PropertyRecord>()
            {
                @Override
                public void process( PropertyRecord record )
                {
                    checkProp( propStore, stringStore, arrayStore, result, record );
                }
            }, propStore, Filter.IN_USE );
            processDynamic( runner, stringStore, result );
        }
    }

    private static void processDynamic( StoreToolRunner runner, final DynamicStoreAccess<?> dynamicStore,
            final Collection<RecordInconsistency> result )
    {
        runner.process( new RecordProcessor<DynamicRecord>()
        {
            @Override
            public void process( DynamicRecord record )
            {
                checkDynamic( dynamicStore, result, record );
            }
        }, dynamicStore, Filter.IN_USE );
    }

    private static void checkNode( RelationshipStoreAccess relStore, PropertyStoreAccess propStore,
            Collection<RecordInconsistency> result, NodeRecord node )
    {
        long relId = node.getNextRel();
        if ( relId != RelationshipStoreAccess.NO_NEXT_RECORD )
        {
            RelationshipRecord rel = relStore.forceGetRecord( relId );
            if ( !rel.inUse() || !( rel.getFirstNode() == node.getId() || rel.getSecondNode() == node.getId() ) )
            {
                result.add( new RecordInconsistency( node, rel ) );
            }
        }
        if ( propStore != null )
        {
            long propId = node.getNextProp();
            if ( propId != PropertyStoreAccess.NO_NEXT_RECORD )
            {
                PropertyRecord prop = propStore.forceGetRecord( propId );
                if ( !prop.inUse() ) result.add( new RecordInconsistency( node, prop ) );
            }
        }
    }

    private static RecordField[] fields = RecordField.values();

    @SuppressWarnings( "boxing" )
    private static void checkRel( RelationshipStoreAccess relStore, NodeStoreAccess nodeStore,
            PropertyStoreAccess propStore, Collection<RecordInconsistency> result, RelationshipRecord rel )
    {
        for ( RecordField field : fields )
        {
            long otherId = field.relOf( rel );
            if ( otherId == field.none )
            {
                Long nodeId = field.nodeOf( rel );
                if ( nodeId != null )
                {
                    NodeRecord node = nodeStore.forceGetRecord( nodeId );
                    if ( !node.inUse() || node.getNextRel() != rel.getId() )
                    {
                        result.add( new RecordInconsistency( rel, node ) );
                    }
                }
            }
            else
            {
                RelationshipRecord other = relStore.forceGetRecord( otherId );
                if ( !other.inUse() || !field.invConsistent( rel, other ) )
                {
                    result.add( new RecordInconsistency( rel, other ) );
                }
            }
        }
        if ( propStore != null )
        {
            long propId = rel.getNextProp();
            if ( propId != PropertyStoreAccess.NO_NEXT_RECORD )
            {
                PropertyRecord prop = propStore.forceGetRecord( propId );
                if ( !prop.inUse() ) result.add( new RecordInconsistency( rel, prop ) );
            }
        }
    }

    @SuppressWarnings( "incomplete-switch" )
    private static void checkProp( PropertyStoreAccess propStore, StringPropertyStoreAccess stringStore,
            ArrayPropertyStoreAccess arrayStore, Collection<RecordInconsistency> result, PropertyRecord prop )
    {
        long nextId = prop.getNextProp();
        if ( nextId != PropertyStoreAccess.NO_NEXT_RECORD )
        {
            PropertyRecord next = propStore.forceGetRecord( nextId );
            if ( !next.inUse() || next.getPrevProp() != prop.getId() )
            {
                result.add( new RecordInconsistency( prop, next ) );
            }
        }
        long prevId = prop.getPrevProp();
        if ( prevId != PropertyStoreAccess.NO_PREV_RECORD )
        {
            PropertyRecord prev = propStore.forceGetRecord( prevId );
            if ( !prev.inUse() || prev.getNextProp() != prop.getId() )
            {
                result.add( new RecordInconsistency( prop, prev ) );
            }
        }
        DynamicStoreAccess<?> store = null;
        if ( prop.getType() == null )
        {
            result.add( new RecordInconsistency( prop, null ) );
        }
        else
        {
            switch ( prop.getType() )
            {
            case ILLEGAL:
                result.add( new RecordInconsistency( prop, null ) );
                break;
            case STRING:
                store = stringStore;
                break;
            case ARRAY:
                store = arrayStore;
                break;
            }
        }
        if ( store != null )
        {
            DynamicRecord block = store.forceGetRecord( prop.getPropBlock() );
            if ( !block.inUse() )
            {
                result.add( new RecordInconsistency( prop, block ) );
            }
        }
    }

    private static void checkDynamic( DynamicStoreAccess<?> store, Collection<RecordInconsistency> result,
            DynamicRecord record )
    {
        long nextId = record.getNextBlock();
        if ( nextId != DynamicStoreAccess.NO_NEXT_RECORD )
        {
            DynamicRecord next = store.forceGetRecord( nextId );
            if ( !next.inUse() || next.getPrevBlock() != record.getId() )
            {
                result.add( new RecordInconsistency( record, next ) );
            }
        }
        long prevId = record.getPrevBlock();
        if ( prevId != DynamicStoreAccess.NO_PREV_RECORD )
        {
            DynamicRecord prev = store.forceGetRecord( prevId );
            if ( !prev.inUse() || prev.getNextBlock() != record.getId() )
            {
                result.add( new RecordInconsistency( record, prev ) );
            }
        }
    }

    @SuppressWarnings( "boxing" )
    private enum RecordField
    {
        FIRST_NEXT( true, RelationshipStoreAccess.NO_NEXT_RECORD )
        {
            @Override
            long relOf( RelationshipRecord rel )
            {
                return rel.getFirstNextRel();
            }

            @Override
            boolean invConsistent( RelationshipRecord rel, RelationshipRecord other )
            {
                long node = getNode( rel );
                if ( other.getFirstNode() == node ) return other.getFirstPrevRel() == rel.getId();
                if ( other.getSecondNode() == node ) return other.getSecondPrevRel() == rel.getId();
                return false;
            }
        },
        FIRST_PREV( true, RelationshipStoreAccess.NO_PREV_RECORD )
        {
            @Override
            long relOf( RelationshipRecord rel )
            {
                return rel.getFirstPrevRel();
            }

            @Override
            Long nodeOf( RelationshipRecord rel )
            {
                return getNode( rel );
            }

            @Override
            boolean invConsistent( RelationshipRecord rel, RelationshipRecord other )
            {
                long node = getNode( rel );
                if ( other.getFirstNode() == node ) return other.getFirstNextRel() == rel.getId();
                if ( other.getSecondNode() == node ) return other.getSecondNextRel() == rel.getId();
                return false;
            }
        },
        SECOND_NEXT( false, RelationshipStoreAccess.NO_NEXT_RECORD )
        {
            @Override
            long relOf( RelationshipRecord rel )
            {
                return rel.getSecondNextRel();
            }

            @Override
            boolean invConsistent( RelationshipRecord rel, RelationshipRecord other )
            {
                long node = getNode( rel );
                if ( other.getFirstNode() == node ) return other.getFirstPrevRel() == rel.getId();
                if ( other.getSecondNode() == node ) return other.getSecondPrevRel() == rel.getId();
                return false;
            }
        },
        SECOND_PREV( false, RelationshipStoreAccess.NO_PREV_RECORD )
        {
            @Override
            long relOf( RelationshipRecord rel )
            {
                return rel.getSecondPrevRel();
            }

            @Override
            Long nodeOf( RelationshipRecord rel )
            {
                return getNode( rel );
            }

            @Override
            boolean invConsistent( RelationshipRecord rel, RelationshipRecord other )
            {
                long node = getNode( rel );
                if ( other.getFirstNode() == node ) return other.getFirstNextRel() == rel.getId();
                if ( other.getSecondNode() == node ) return other.getSecondNextRel() == rel.getId();
                return false;
            }
        };

        private final boolean first;
        final long none;

        private RecordField( boolean first, long none )
        {
            this.first = first;
            this.none = none;
        }

        abstract boolean invConsistent( RelationshipRecord rel, RelationshipRecord other );

        long getNode( RelationshipRecord rel )
        {
            return first ? rel.getFirstNode() : rel.getSecondNode();
        }

        abstract long relOf( RelationshipRecord rel );

        Long nodeOf( RelationshipRecord rel )
        {
            return null;
        }
    }

    private final Abstract64BitRecord record, referred;

    private RecordInconsistency( Abstract64BitRecord record, Abstract64BitRecord referred )
    {
        this.record = record;
        this.referred = referred;
    }

    @Override
    public String toString()
    {
        if ( referred == null )
        {
            return "Internally inconsistent: " + record;
        }
        else
        {
            return "Inconsistent: " + record + " with regards to " + referred;
        }
    }
}
