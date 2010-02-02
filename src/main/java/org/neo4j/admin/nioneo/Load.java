package org.neo4j.admin.nioneo;

import java.rmi.RemoteException;

import org.neo4j.admin.nioneo.store.NodeRecord;
import org.neo4j.admin.nioneo.store.NodeStore;
import org.neo4j.admin.nioneo.store.RelationshipStore;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.OptionValueType;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.impl.AbstractApp.OptionContext;

public class Load extends NioneoApp
{

    public Load()
    {
        super();
        this.addValueType( "n", new OptionContext( OptionValueType.NONE,
            "Load node record" ) );
        this.addValueType( "r", new OptionContext( OptionValueType.NONE,
            "Load rel record" ) );
        this.addValueType( "p", new OptionContext( OptionValueType.NONE,
            "Load property record" ) );
        
    }
    
    public String execute( AppCommandParser parser, Session session, Output out )
            throws ShellException
    {
        String arg = parser.arguments().get( 0 );
        int id = Integer.parseInt( arg );
        String response = "Record " + id + " loaded";
        if ( parser.options().containsKey( "n" ) )
        {
            getServer().setRecord( getServer().getNodeStore().forceGetRecord( id ) );
        }
        else if ( parser.options().containsKey( "r" ) )
        {
            getServer().setRecord( getServer().getRelStore().forceGetRecord( id ) );
        }
        else if ( parser.options().containsKey( "p" ) )
        {
            getServer().setRecord( getServer().getPropStore().forceGetRecord( id ) );
        }
        else
        {
            response = "Error: Specify type of of record";
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
