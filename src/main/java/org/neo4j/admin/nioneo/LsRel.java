package org.neo4j.admin.nioneo;

import java.rmi.RemoteException;

import org.neo4j.admin.nioneo.store.RelationshipRecord;
import org.neo4j.admin.nioneo.store.RelationshipStore;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;

public class LsRel extends NioneoApp
{

    public String execute( AppCommandParser parser, Session session, Output out )
            throws ShellException
    {
        String arg = parser.arguments().get( 0 );
        int id = Integer.parseInt( arg );
        RelationshipStore relStore = getServer().getRelStore();
        RelationshipRecord record = relStore.forceGetRecord( id );
        StringBuffer buf = new StringBuffer( "rel record #" );
        buf.append( record.getId() ).append( " [" );
        buf.append( record.inUse() );
        buf.append( "|fN " ).append( record.getFirstNode() );
        buf.append( "|sN " ).append( record.getSecondNode() );
        buf.append( "|t " ).append( record.getType() );
        buf.append( "|fpN " ).append( record.getFirstPrevRel() );
        buf.append( "|fnN " ).append( record.getFirstNextRel() );
        buf.append( "|spN " ).append( record.getSecondPrevRel() );
        buf.append( "|snN " ).append( record.getSecondNextRel() );
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
