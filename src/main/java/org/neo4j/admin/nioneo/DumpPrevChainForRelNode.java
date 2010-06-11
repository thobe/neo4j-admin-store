package org.neo4j.admin.nioneo;

import java.rmi.RemoteException;

import org.neo4j.admin.nioneo.store.Record;
import org.neo4j.admin.nioneo.store.RelationshipRecord;
import org.neo4j.admin.nioneo.store.RelationshipStore;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;

public class DumpPrevChainForRelNode extends NioneoApp
{

    public String execute( AppCommandParser parser, Session session, Output out )
            throws ShellException
    {
        String arg = parser.arguments().get( 0 );
        int relId = Integer.parseInt( arg );
        int nodeId = Integer.parseInt( parser.arguments().get( 1 ) );
        RelationshipStore relStore = getServer().getRelStore();
        String hit = "Not found";
        StringBuffer hits = new StringBuffer();
        int prevRelId = relId;
        do
        {
            RelationshipRecord record = relStore.getRecord( prevRelId );
            hits.append( prevRelId + " " );
            if ( record.getFirstNode() == nodeId )
            {
                prevRelId = record.getFirstPrevRel();
            }
            else if ( record.getSecondNode() == nodeId )
            {
                prevRelId = record.getSecondPrevRel();
            }
        } while ( prevRelId != Record.NO_PREV_RELATIONSHIP.intValue() );
        if ( hits.length() > 0 )
        {
            hit = hits.toString();
        }
        try
        {
            out.println( hit );
        }
        catch ( RemoteException e )
        {
            throw new ShellException( e );
        }
        return null;
    }

}
