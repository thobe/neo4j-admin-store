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
package org.neo4j.admin.tool.mkcopy;

import static org.neo4j.admin.tool.RecordProcessor.Factory.nodeProcessor;
import static org.neo4j.admin.tool.RecordProcessor.Factory.relationshipProcessor;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.admin.tool.RecordProcessor.NodeProcessor;
import org.neo4j.admin.tool.RecordProcessor.RelationshipProcessor;
import org.neo4j.admin.tool.SimpleStoreTool;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.impl.batchinsert.BatchInserter;
import org.neo4j.kernel.impl.batchinsert.BatchInserterImpl;
import org.neo4j.kernel.impl.nioneo.store.Filter;
import org.neo4j.kernel.impl.nioneo.store.IndexKeyStoreAccess;
import org.neo4j.kernel.impl.nioneo.store.InvalidRecordException;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyStoreAccess;
import org.neo4j.kernel.impl.nioneo.store.PropertyType;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeStoreAccess;

public class Main extends SimpleStoreTool implements NodeProcessor, RelationshipProcessor
{
    private final BatchInserter target;
    private final RelationshipTypeStoreAccess types;
    private final IndexKeyStoreAccess propKeys;
    private final PropertyStoreAccess props;

    Main( String[] args )
    {
        super( args );
        this.target = new BatchInserterImpl( args[1] );
        this.types = store.getTypeStore();
        this.props = store.getPropStore();
        this.propKeys = store.getIndexKeyStore();
    }
    
    @Override
    protected void shutdown()
    {
        target.shutdown();
        super.shutdown();
    }

    public static void main( String[] args ) throws Throwable
    {
        main( Main.class, args );
    }

    @Override
    protected void run() throws Throwable
    {
        process( nodeProcessor( this ), store.getNodeStore(), Filter.IN_USE );
        process( relationshipProcessor( this ), store.getRelStore(), Filter.IN_USE );
    }

    @Override
    public void processNode( NodeRecord record )
    {
        if ( record.getId() == 0 )
        { // node 0 already exists, even with the batch inserter
            target.setNodeProperties( record.getId(), properties( record.getNextProp() ) );
        }
        else
        {
            target.createNode( record.getId(), properties( record.getNextProp() ) );
        }
    }

    @Override
    public void processRelationship( RelationshipRecord record )
    {
        RelationshipType type = type( record.getType() );
        Map<String, Object> properties = properties( record.getNextProp() );
        try
        {
            target.createRelationship( record.getFirstNode(), record.getSecondNode(), type, properties );
        }
        catch ( InvalidRecordException e )
        {
            System.err.println( "Failed to recreate " + record + ": " + e );
        }
    }

    private RelationshipType type( int type )
    {
        return DynamicRelationshipType.withName( types.getType( type ) );
    }

    private Map<String, Object> properties( long prop )
    {
        if ( PropertyStoreAccess.NO_NEXT_RECORD == prop ) return null;
        Map<String, Object> result = new HashMap<String, Object>();
        while ( PropertyStoreAccess.NO_NEXT_RECORD != prop )
        {
            PropertyRecord record = props.forceGetRecord( prop );
            prop = record.getNextProp();
            PropertyType type = record.getType();
            if ( type == null ) continue;
            String key = propKeys.getKey( record.getKeyIndexId() );
            Object value = type.getValue( record, props.getStore() );
            result.put( key, value );
        }
        return result;
    }
}
