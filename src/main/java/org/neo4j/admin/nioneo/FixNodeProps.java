package org.neo4j.admin.nioneo;

import java.rmi.RemoteException;

import org.neo4j.admin.nioneo.store.DynamicRecord;
import org.neo4j.admin.nioneo.store.NodeRecord;
import org.neo4j.admin.nioneo.store.NodeStore;
import org.neo4j.admin.nioneo.store.PropertyRecord;
import org.neo4j.admin.nioneo.store.PropertyStore;
import org.neo4j.admin.nioneo.store.PropertyType;
import org.neo4j.admin.nioneo.store.Record;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;

public class FixNodeProps extends NioneoApp
{

    public String execute( AppCommandParser parser, Session session, Output out )
            throws ShellException
    {
        String arg = parser.arguments().get( 0 );
        int id = Integer.parseInt( arg );
        NodeStore nodeStore = getServer().getNodeStore();
        PropertyStore propStore = getServer().getPropStore();
        NodeRecord nodeRecord = nodeStore.forceGetRecord( id );
        int nextProp = nodeRecord.getNextProp();
        int startProp = nextProp;
        int prevProp = Record.NO_PREVIOUS_PROPERTY.intValue();
        while ( nextProp != Record.NO_NEXT_PROPERTY.intValue() )
        {
            PropertyRecord propRecord = propStore.forceGetRecord( nextProp );
            if ( !propRecord.inUse()  )
            {
                nextProp = Record.NO_NEXT_PROPERTY.intValue();
                if ( startProp == propRecord.getId() )
                {
                    int nodeNextProp = Record.NO_NEXT_PROPERTY.intValue();
                    nodeRecord.setNextProp( nodeNextProp );
                    nodeStore.forceUpdateRecord( nodeRecord );
                }
                if ( prevProp != Record.NO_PREVIOUS_PROPERTY.intValue() )
                {
                    PropertyRecord prev = propStore.forceGetRecord( prevProp );
                    prev.setNextProp( Record.NO_NEXT_PROPERTY.intValue() );
                    propStore.forceUpdateRecord( prev );
                }
            }
            else 
            {
                if ( propRecord.getType() == PropertyType.STRING )
                {
                    DynamicRecord dr = getServer().getStringStore().forceGetRecord( 
                        (int) propRecord.getPropBlock() );
                    if ( !dr.inUse() )
                    {
                        propRecord.setInUse( false );
                        propStore.forceUpdateRecord( propRecord );
                        continue;
                    }
                    if ( dr.getPrevBlock() != Record.NO_PREV_BLOCK.intValue() )
                    {
                        propRecord.setInUse( false );
                        propStore.forceUpdateRecord( propRecord );
                        continue;
                    }
                }
                else if ( propRecord.getType() == PropertyType.ARRAY )
                {
                    DynamicRecord dr = getServer().getArrayStore().forceGetRecord( 
                        (int) propRecord.getPropBlock() );
                    if ( !dr.inUse() )
                    {
                        propRecord.setInUse( false );
                        propStore.forceUpdateRecord( propRecord );
                        continue;
                    }
                    if ( dr.getPrevBlock() != Record.NO_PREV_BLOCK.intValue() )
                    {
                        propRecord.setInUse( false );
                        propStore.forceUpdateRecord( propRecord );
                        continue;
                    }
                }
                if ( propRecord.getPrevProp() != prevProp )
                {
                    propRecord.setPrevProp( prevProp );
                    propStore.forceUpdateRecord( propRecord );
                }
                prevProp = propRecord.getId();
                nextProp = propRecord.getNextProp();
            }
        }
        try
        {
            out.println( "Done" );
        }
        catch ( RemoteException e )
        {
            throw new ShellException( e );
        }
        return null;
    }

}
