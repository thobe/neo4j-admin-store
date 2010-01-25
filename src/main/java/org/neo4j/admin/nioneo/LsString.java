package org.neo4j.admin.nioneo;

import java.rmi.RemoteException;

import org.neo4j.admin.nioneo.store.DynamicRecord;
import org.neo4j.admin.nioneo.store.DynamicStringStore;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;

public class LsString extends NioneoApp
{

    public String execute( AppCommandParser parser, Session session, Output out )
            throws ShellException
    {
        String arg = parser.arguments().get( 0 );
        int id = Integer.parseInt( arg );
        DynamicStringStore stringStore = getServer().getStringStore();
        DynamicRecord record = stringStore.forceGetRecord( id );
        StringBuffer buf = new StringBuffer( "str block #" );
        buf.append( record.getId() ).append( " [" );
        buf.append( record.inUse() );
        buf.append( "|pB " ).append( record.getPrevBlock() );
        buf.append( "|s " ).append( record.getLength() );
        buf.append( "|nB " ).append( record.getNextBlock() );
        buf.append( "|" );
        byte data[] = record.getData();
        for ( int i = 0; i < 3; i++ )
        {
            buf.append( " " ).append( data[i] );
        }
        buf.append( "...]" );
        try
        {
            out.println( buf );
        }
        catch ( RemoteException e )
        {
            throw new ShellException( e );
        }
        return null;
    }

}
