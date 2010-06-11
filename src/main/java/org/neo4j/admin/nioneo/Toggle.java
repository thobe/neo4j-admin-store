package org.neo4j.admin.nioneo;

import java.rmi.RemoteException;

import org.neo4j.admin.nioneo.store.AbstractRecord;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;

public class Toggle extends NioneoApp
{
    public String execute( AppCommandParser parser, Session session, Output out )
            throws ShellException
    {
        AbstractRecord record = getServer().getCurrentRecord();
        String response;
        if ( record == null )
        {
            response = "Error: no record loaded";
        }
        else if ( record.inUse() )
        {
            record.setInUse( false );
            response = "Marked as not in use";
        }
        else 
        {
            record.setInUse( true );
            response = "Marked as in use";
        }
        try
        {
            out.println( response );
        }
        catch ( RemoteException e )
        {
            throw new ShellException( e );
        }
        return null;
    }
}
