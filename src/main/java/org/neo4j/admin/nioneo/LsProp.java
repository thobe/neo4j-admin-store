package org.neo4j.admin.nioneo;

import java.rmi.RemoteException;

import org.neo4j.admin.nioneo.store.PropertyRecord;
import org.neo4j.admin.nioneo.store.PropertyStore;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;

public class LsProp extends NioneoApp
{

    public String execute( AppCommandParser parser, Session session, Output out )
            throws ShellException
    {
        String arg = parser.arguments().get( 0 );
        int id = Integer.parseInt( arg );
        PropertyStore propStore = getServer().getPropStore();
        PropertyRecord record = propStore.forceGetRecord( id );
        StringBuffer buf = new StringBuffer( "prop record #" );
        buf.append( record.getId() ).append( " [" );
        buf.append( record.inUse() );
        buf.append( "|type " ).append( record.getType() );
        buf.append( "|key " ).append( record.getKeyIndexId() );
        buf.append( "|value " ).append( record.getPropBlock() );
        buf.append( "|pP " ).append( record.getPrevProp() );
        buf.append( "|nP " ).append( record.getNextProp() );
        buf.append( "]" );
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
