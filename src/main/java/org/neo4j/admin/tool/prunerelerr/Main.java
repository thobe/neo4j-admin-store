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
package org.neo4j.admin.tool.prunerelerr;

import org.neo4j.admin.tool.SimpleStoreTool;

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
    protected void run() throws Throwable
    {
        // TODO tobias Oct 20, 2011: Implement SimpleStoreTool.run()
        if ( true ) throw new UnsupportedOperationException( "Not implemented" );

    }
}
