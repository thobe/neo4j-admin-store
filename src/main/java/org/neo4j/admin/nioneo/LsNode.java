package org.neo4j.admin.nioneo;

import java.rmi.RemoteException;

import org.neo4j.admin.nioneo.store.NodeRecord;
import org.neo4j.admin.nioneo.store.NodeStore;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;

public class LsNode extends NioneoApp
{

    public String execute( AppCommandParser parser, Session session, Output out )
            throws ShellException
    {
        String arg = parser.arguments().get( 0 );
        int id = Integer.parseInt( arg );
        NodeStore nodeStore = getServer().getNodeStore();
        NodeRecord record = nodeStore.forceGetRecord( id );
        StringBuffer buf = new StringBuffer( "node record #" );
        buf.append( record.getId() ).append( " [" );
        buf.append( record.inUse() );
        buf.append( "|nR " ).append( record.getNextRel() );
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
