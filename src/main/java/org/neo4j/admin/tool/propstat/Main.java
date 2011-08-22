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
package org.neo4j.admin.tool.propstat;

import java.util.EnumMap;
import java.util.Map;

import org.neo4j.admin.tool.RecordProcessor;
import org.neo4j.admin.tool.SimpleStoreTool;
import org.neo4j.helpers.Args;
import org.neo4j.kernel.impl.nioneo.store.Filter;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyType;

public class Main extends SimpleStoreTool implements RecordProcessor<PropertyRecord>, Filter<PropertyRecord>
{
    private final Map<PropertyType, Statistics> statistics = new EnumMap<PropertyType, Statistics>( PropertyType.class );

    private Main( String[] args )
    {
        super( args );
        Args arg = new Args( args );
        boolean useLongVersion = arg.getBoolean( "long", false );
        for ( PropertyType type : PropertyType.values() )
        {
            final Statistics stat;
            switch ( type )
            {
            case SHORT_STRING:
                stat = new StringStatistics.ShortString();
                break;
            case STRING:
                stat = new StringStatistics.DynamicString( store.getStringPropertyStore(), store.getPropStore(), useLongVersion );
                break;
            case ARRAY:
                stat = new ArrayStatistics( store.getArrayPropertyStore(), store.getPropStore() );
                break;
            default:
                stat = new Statistics( store.getPropStore(), type.name() );
            }
            statistics.put( type, stat );
        }
    }

    @Override
    public String toString()
    {
        return "Gathering property statistics";
    }

    @SuppressWarnings( "unchecked" )
    @Override
    protected void run()
    {
        process( this, store.getPropStore(), this );
        for ( Statistics stat : statistics.values() )
        {
            if ( stat.hasData() ) System.out.println( stat );
        }
    }

    @Override
    public void process( PropertyRecord record )
    {
        Statistics stats = statistics.get( record.getType() );
        stats.add( stats.extractValue( record ), record );
    }

    @Override
    public boolean accept( PropertyRecord record )
    {
        return record.inUse();
    }

    public static void main( String[] args ) throws Throwable
    {
        main( Main.class, args );
    }
}
