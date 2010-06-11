package org.neo4j.admin.nioneo;

import java.rmi.RemoteException;

import org.neo4j.admin.nioneo.store.AbstractRecord;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;

public class Ls extends NioneoApp
{

    public Ls()
    {
        super();
    }
    
    public String execute( AppCommandParser parser, Session session, Output out )
            throws ShellException
    {
        AbstractRecord record = getServer().getCurrentRecord();
        String response = "No record loaded";
        if ( record != null )
        {
            response = Util.getRecordString( record );
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
