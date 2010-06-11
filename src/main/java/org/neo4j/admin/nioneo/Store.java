package org.neo4j.admin.nioneo;

import java.rmi.RemoteException;

import org.neo4j.admin.nioneo.store.AbstractRecord;
import org.neo4j.admin.nioneo.store.DynamicRecord;
import org.neo4j.admin.nioneo.store.NodeRecord;
import org.neo4j.admin.nioneo.store.PropertyRecord;
import org.neo4j.admin.nioneo.store.PropertyType;
import org.neo4j.admin.nioneo.store.RelationshipRecord;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;

public class Store extends NioneoApp
{

    public String execute( AppCommandParser parser, Session session, Output out )
            throws ShellException
    {
        AbstractRecord record = getServer().getCurrentRecord();
        String response;
        if ( record == null )
        {
            response = "Error: No record loaded";
        }
        else if ( record instanceof NodeRecord )
        {
            getServer().getNodeStore().forceUpdateRecord( (NodeRecord) record );
            response = "NodeRecord #" + record.getId() + " stored";
        }
        else if ( record instanceof RelationshipRecord )
        {
            getServer().getRelStore().forceUpdateRecord( (RelationshipRecord) record );
            response = "RelRecord #" + record.getId() + " stored";
        }
        else if ( record instanceof PropertyRecord )
        {
            getServer().getPropStore().forceUpdateRecord( (PropertyRecord) record );
            response = "PropRecord #" + record.getId() + " stored";
        }
        else if ( record instanceof DynamicRecord )
        {
            DynamicRecord rec = (DynamicRecord )record;
            if ( rec.getType() == PropertyType.STRING.intValue() )
            {
                getServer().getPropStore().getStringStore().forceUpdate( rec );
                response = "StringRecord #" + record.getId() + " stored";

            }
            else if ( rec.getType() == PropertyType.ARRAY.intValue() )
            {
                getServer().getPropStore().getArrayStore().forceUpdate( rec );
                response = "ArrayRecord #" + record.getId() + " stored";

            }
            else
            {
                response = "Error: unkown record type " + record;
            }
        }
        else
        {
            response = "Error: unkown record type " + record;
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
