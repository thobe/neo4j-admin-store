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
package org.neo4j.admin.tool.prunenoderr;

import org.neo4j.admin.tool.RecordProcessor;
import org.neo4j.admin.tool.SimpleStoreTool;
import org.neo4j.kernel.impl.nioneo.store.Filter;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeStoreAccess;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipStoreAccess;

public class Main extends SimpleStoreTool
{
    Main( String[] args )
    {
        super( args );
    }

    public static void main( String[] args ) throws Throwable
    {
        main( Main.class, args );
    }

    @Override
    @SuppressWarnings( "unchecked" )
    protected void run() throws Throwable
    {
        final NodeStoreAccess nodes = store.getNodeStore();
        final RelationshipStoreAccess rels = store.getRelStore();
        process( new RecordProcessor<NodeRecord>()
        {
            @Override
            public void process( NodeRecord record )
            {
                if ( record.getNextRel() == RelationshipStoreAccess.NO_NEXT_RECORD ) return;
                RelationshipRecord first = rels.forceGetRecord( record.getNextRel() );
                if ( first.inUse() )
                {
                    if ( first.getFirstNode() == record.getId() || first.getSecondNode() == record.getId() ) return;
                }
                System.out.println( record );
                record.setNextRel( RelationshipStoreAccess.NO_NEXT_RECORD );
                nodes.forceUpdateRecord( record );
            }
        }, nodes, Filter.IN_USE );
    }

}
