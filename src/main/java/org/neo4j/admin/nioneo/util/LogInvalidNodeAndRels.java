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
package org.neo4j.admin.nioneo.util;

import java.util.HashSet;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.EmbeddedGraphDatabase;

public class LogInvalidNodeAndRels
{
    public static void main ( String[] args )
    {
        if ( args.length != 1 )
        {
            System.out.println( "Usage: LogInvalidNodeAndRels <neo-store-dir>" );
            return;
        }
        GraphDatabaseService gdb = new EmbeddedGraphDatabase( args[0] );
        for ( Node node : gdb.getAllNodes() )
        {
            try
            {
                HashSet<Long> relIds = new HashSet<Long>();
                for ( Relationship rel : node.getRelationships() )
                {
                    if ( relIds.contains( rel.getId() ) )
                    {
                        System.out.println( "Loop problem relchain for " + node + " [" + rel + " multiple times]" );
                        break;
                    }
                    if ( rel.getStartNode().equals( node ) )
                    {
                        checkProperties( rel );
                    }
                }
            }
            catch ( Throwable t )
            {
                System.out.println( "RelChain problem with " + node + " [" + t.getMessage() + "]" );
            }
            checkProperties( node );
        }
        gdb.shutdown();
    }
    
    private static void checkProperties( PropertyContainer entity )
    {
        try
        {
            for ( String key : entity.getPropertyKeys() )
            {
                entity.getProperty( key );
            }
        }
        catch ( Throwable t )
        {
            System.out.println( "PropChain problem with " + entity  + " [" + t.getMessage() + "]" );
        }
    }
}
