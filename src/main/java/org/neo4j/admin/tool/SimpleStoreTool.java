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
package org.neo4j.admin.tool;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.neo4j.kernel.impl.nioneo.store.Abstract64BitRecord;
import org.neo4j.kernel.impl.nioneo.store.CommonAbstractStore;
import org.neo4j.kernel.impl.nioneo.store.Filter;
import org.neo4j.kernel.impl.nioneo.store.GraphDatabaseStore;
import org.neo4j.kernel.impl.nioneo.store.StoreAccess;

public abstract class SimpleStoreTool implements StoreToolRunner
{
    protected final GraphDatabaseStore store;

    protected SimpleStoreTool( String[] args )
    {
        if ( args.length == 0 )
        {
            throw new IllegalArgumentException( "USAGE: java -jar " + jar( getClass() ) + " <path to neo4j store dir>" );
        }
        this.store = new GraphDatabaseStore( args[0] );
    }

    public static <SST extends SimpleStoreTool> void main( Class<SST> main, String... args ) throws Throwable
    {
        final Constructor<SST> init;
        try
        {
            init = main.getDeclaredConstructor( String[].class );
        }
        catch ( Exception e )
        {
            System.err.println( main.getName() + " cannot use the default main implementation" );
            e.printStackTrace();
            return;
        }
        init.setAccessible( true );
        if ( args == null ) args = new String[0];
        final SST instance;
        try
        {
            instance = init.newInstance( new Object[] { args } );
        }
        catch ( InvocationTargetException e )
        {
            Throwable failure = e.getTargetException();
            if ( failure instanceof IllegalArgumentException )
            {
                System.err.println( failure.getMessage() );
                return;
            }
            else
            {
                throw failure;
            }
        }
        catch ( Exception e )
        {
            System.err.println( main.getName() + " cannot use the default main implementation" );
            e.printStackTrace();
            return;
        }
        instance.prepare();
        try
        {
            instance.run();
        }
        finally
        {
            instance.shutdown();
        }
    }

    protected abstract void run() throws Throwable;

    protected void prepare()
    {
        store.makeStoreOk();
    }

    protected void shutdown()
    {
        store.shutdown();
    }

    @Override
    public GraphDatabaseStore store()
    {
        return store;
    }

    public final <T extends CommonAbstractStore, R extends Abstract64BitRecord> void process(
            RecordProcessor<R> processor, StoreAccess<T, R> store, Filter<? super R>... filters )
    {
        long highId = store.getHighId();
        System.err.printf( "%s for %s records%n", processor, Long.toString( highId ) );
        int lastPercent = 0;
        for ( R record : store.scan( filters ) )
        {
            processor.process( record );
            int permille = (int) ( ( record.getId() * 1000L ) / highId );
            if ( permille != lastPercent ) progress( lastPercent = permille );
        }
        if ( lastPercent != 1000 ) progress( 1000 );
    }

    private static void progress( int permille )
    {
        if ( permille % 100 == 0 )
            System.err.printf( "%3s%%%n", Integer.toString( permille / 10 ) );
        else if ( permille % 5 == 0 ) System.err.print( "." );
    }

    private static String jar( Class<?> type )
    {
        return type.getProtectionDomain().getCodeSource().getLocation().getFile();
    }
}
