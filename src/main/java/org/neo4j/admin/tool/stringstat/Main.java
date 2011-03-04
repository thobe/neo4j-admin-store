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

package org.neo4j.admin.tool.stringstat;

import org.neo4j.admin.tool.SimpleStoreTool;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.Filter;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.StringPropertyStoreAccess;

class Main extends SimpleStoreTool implements Filter<DynamicRecord>
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
    protected void run()
    {
        StringBuilder report = new StringBuilder( "<STRING STORE STATISTICS>\n" );
        StringPropertyStoreAccess strings = store.getStringPropertyStore();
        Processor step = new ComputeFrequencies( strings, report );
        while ( step != null )
        {
            process( step, strings, this );
            step = step.next();
        }
        System.out.print( report.append( "</STRING STORE STATISTICS>\n" ).toString() );
        System.out.flush();
    }

    private static final int NO_NEXT_BLOCK = Record.NO_NEXT_BLOCK.intValue(),
            NO_PREV_BLOCK = Record.NO_PREV_BLOCK.intValue();

    public boolean accept( DynamicRecord record )
    {
        return record.inUse() && record.getNextBlock() == NO_NEXT_BLOCK && record.getPrevBlock() == NO_PREV_BLOCK;
    }
}
