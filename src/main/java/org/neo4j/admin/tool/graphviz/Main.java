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
package org.neo4j.admin.tool.graphviz;

import static org.neo4j.admin.tool.RecordProcessor.Factory.nodeProcessor;
import static org.neo4j.admin.tool.RecordProcessor.Factory.propertyProcessor;
import static org.neo4j.admin.tool.RecordProcessor.Factory.relationshipProcessor;

import org.neo4j.admin.tool.RecordProcessor;
import org.neo4j.admin.tool.SimpleStoreTool;
import org.neo4j.kernel.impl.nioneo.store.Filter;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;

public class Main extends SimpleStoreTool implements RecordProcessor.NodeProcessor,
        RecordProcessor.RelationshipProcessor, RecordProcessor.PropertyProcessor
{
    private final StringBuilder result;
    private boolean properties;

    Main( String[] args )
    {
        super( args );
        result = new StringBuilder( "digraph RelationshipStore {\n" );
    }

    public static void main( String[] args ) throws Throwable
    {
        main( Main.class, args );
    }

    @Override
    public String toString()
    {
        return "generate graphviz";
    }

    private void emit( Object... data )
    {
        result.append( "  " );
        for ( Object item : data )
        {
            result.append( item );
        }
        result.append( "\n" );
    }

    private static final int NO_NEXT_BLOCK = Record.NO_NEXT_BLOCK.intValue(),
            NO_PREV_BLOCK = Record.NO_PREV_BLOCK.intValue();

    @Override
    @SuppressWarnings( "unchecked" )
    protected void run()
    {
        properties = true; // TODO: parameters for the tool!
        process( nodeProcessor( this ), store.getNodeStore(), Filter.IN_USE );
        process( relationshipProcessor( this ), store.getRelStore(), Filter.IN_USE );
        if ( properties ) process( propertyProcessor( this ), store.getPropStore(), Filter.IN_USE );
        result.append( "}\n" );
        System.out.print( result.toString() );
    }

    @SuppressWarnings( "boxing" )
    public void processNode( NodeRecord record )
    {
        emit( "node", record.getId(), " []" );
        if ( record.getNextRel() != NO_NEXT_BLOCK )
            emit( "node", record.getId(), " -> rel", record.getNextRel(), " [label=firstRel]" );
        if ( properties && record.getNextProp() != NO_NEXT_BLOCK )
            emit( "node", record.getId(), " -> prop", record.getNextProp(), " [label=firstProp]" );
    }

    @SuppressWarnings( "boxing" )
    public void processRelationship( RelationshipRecord record )
    {
        emit( "rel", record.getId(), " [shape=record]" );
        if ( record.getFirstNextRel() != NO_NEXT_BLOCK )
            emit( "rel", record.getId(), " -> rel", record.getFirstNextRel(), " [label=firstNext]" );
        if ( record.getFirstPrevRel() != NO_PREV_BLOCK )
            emit( "rel", record.getId(), " -> rel", record.getFirstPrevRel(), " [label=firstPrev]" );
        if ( record.getSecondNextRel() != NO_NEXT_BLOCK )
            emit( "rel", record.getId(), " -> rel", record.getSecondNextRel(), " [label=secondNext]" );
        if ( record.getSecondPrevRel() != NO_PREV_BLOCK )
            emit( "rel", record.getId(), " -> rel", record.getSecondPrevRel(), " [label=secondPrev]" );
        emit( "rel", record.getId(), " -> node", record.getFirstNode(), " [label=first]" );
        emit( "rel", record.getId(), " -> node", record.getSecondNode(), " [label=second]" );
        if ( properties ) emit( "rel", record.getId(), " -> prop", record.getNextProp(), " [label=firstProp]" );
    }

    @SuppressWarnings( "boxing" )
    public void processProperty( PropertyRecord record )
    {
        emit( "prop", record.getId(), " [shape=Mrecord, label=\"id=", record.getId(), " type=", record.getType(), "\"]" );
        if ( record.getNextProp() != NO_NEXT_BLOCK )
            emit( "prop", record.getId(), " -> prop", record.getNextProp(), " [label=next]" );
        if ( record.getPrevProp() != NO_NEXT_BLOCK )
            emit( "prop", record.getId(), " -> prop", record.getPrevProp(), " [label=prev]" );
    }
}
