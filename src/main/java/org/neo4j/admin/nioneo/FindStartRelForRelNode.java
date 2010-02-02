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

public class FindStartRelForRelNode extends NioneoApp
{

    public String execute( AppCommandParser parser, Session session, Output out )
            throws ShellException
    {
        String arg = parser.arguments().get( 0 );
        int relId = Integer.parseInt( arg );
        int nodeId = Integer.parseInt( parser.arguments().get( 1 ) );
        RelationshipStore relStore = getServer().getRelStore();
        String hit = "Not found";
        int nextRelId = relId;
        int prevRelId = -1;
        boolean error = false;
        do
        {
            RelationshipRecord record = relStore.getRecord( nextRelId );
            if ( record.getFirstNode() == nodeId )
            {
                prevRelId = nextRelId;
                nextRelId = record.getFirstPrevRel();
            }
            else if ( record.getSecondNode() == nodeId )
            {
                prevRelId = nextRelId;
                nextRelId = record.getSecondPrevRel();
            }
            else
            {
                hit = "Error going from rel " + prevRelId + " to " + nextRelId;
                nextRelId = Record.NO_PREV_RELATIONSHIP.intValue();
                error = true;
            }
        } while ( nextRelId != Record.NO_PREV_RELATIONSHIP.intValue() );
        if ( !error )
        {
            hit = "rel record # " + prevRelId;
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
