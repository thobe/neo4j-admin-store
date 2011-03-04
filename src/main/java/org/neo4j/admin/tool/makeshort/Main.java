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
package org.neo4j.admin.tool.makeshort;

import org.neo4j.admin.tool.RecordProcessor;
import org.neo4j.admin.tool.SimpleStoreTool;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.Filter;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyType;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.ShortStringEncoding;
import org.neo4j.kernel.impl.nioneo.store.StringPropertyStoreAccess;

public class Main extends SimpleStoreTool implements RecordProcessor<PropertyRecord>, Filter<PropertyRecord>
{
    private int converted = 0, total = 0;

    Main( String[] args )
    {
        super( args );
    }

    @Override
    public String toString()
    {
        return "Converting string properties";
    }

    @Override
    @SuppressWarnings( { "unchecked", "boxing" } )
    protected void run() throws Throwable
    {
        process( this, store.getPropStore(), this );
        System.out.printf( "Converted %s (of %s) string properties to short string properties%n", converted, total );
        System.out.println( "Rebuilding id generators for the string property store" );
        store.getStringPropertyStore().rebuildIdGenerators();
    }

    public static void main( String[] args ) throws Throwable
    {
        main( Main.class, args );
    }

    @Override
    public boolean accept( PropertyRecord record )
    {
        return record.inUse() && record.getType() == PropertyType.STRING;
    }

    private static final int NO_NEXT_BLOCK = Record.NO_NEXT_BLOCK.intValue();

    @Override
    public void process( PropertyRecord record )
    {
        total++;
        StringPropertyStoreAccess strings = store.getStringPropertyStore();
        DynamicRecord string = strings.forceGetRecord( (int) record.getPropBlock() );
        if ( string.getNextBlock() == NO_NEXT_BLOCK )
        {
            if ( ShortStringEncoding.store( record, strings.toString( string ) ) )
            {
                string.setInUse( false );
                strings.forceUpdate( string );
                store.getPropStore().forceUpdateRecord( record );
                converted++;
            }
        }
    }
}
