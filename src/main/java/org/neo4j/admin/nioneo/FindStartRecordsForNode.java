package org.neo4j.admin.nioneo;

import java.rmi.RemoteException;

import org.neo4j.admin.nioneo.store.NodeRecord;
import org.neo4j.admin.nioneo.store.NodeStore;
import org.neo4j.admin.nioneo.store.Record;
import org.neo4j.admin.nioneo.store.RelationshipRecord;
import org.neo4j.admin.nioneo.store.RelationshipStore;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;

public class FindStartRecordsForNode extends NioneoApp
{

    public String execute( AppCommandParser parser, Session session, Output out )
            throws ShellException
    {
        String arg = parser.arguments().get( 0 );
        int id = Integer.parseInt( arg );
        RelationshipStore relStore = getServer().getRelStore();
        int maxRelId = (int) relStore.getHighId();
        String hit = "Not found";
        StringBuffer relHits = new StringBuffer();
        for ( int i = 0; i < maxRelId; i++ )
        {
            RelationshipRecord record = relStore.forceGetRecord( i );
            if ( record.getFirstNode() == id && record.getFirstPrevRel() == Record.NO_PREV_RELATIONSHIP.intValue() )
            {
                relHits.append( "rel record # " + i + " " );   
            }
            if ( record.getSecondNode() == id && record.getSecondPrevRel() == Record.NO_PREV_RELATIONSHIP.intValue() )
            {
                relHits.append( "rel record # " + i + " " );   
            }
        }
        if ( relHits.length() > 0 )
        {
            hit = relHits.toString();
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
