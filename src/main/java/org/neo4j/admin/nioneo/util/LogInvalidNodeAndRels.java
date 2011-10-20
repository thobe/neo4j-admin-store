package org.neo4j.admin.nioneo.util;

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
        }
        GraphDatabaseService gdb = new EmbeddedGraphDatabase( args[0] );
        for ( Node node : gdb.getAllNodes() )
        {
            try
            {
                for ( Relationship rel : node.getRelationships() )
                {
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
