package org.neo4j.admin.nioneo;

import java.rmi.RemoteException;

import org.neo4j.admin.nioneo.store.AbstractRecord;
import org.neo4j.admin.nioneo.store.NodeRecord;
import org.neo4j.admin.nioneo.store.NodeStore;
import org.neo4j.admin.nioneo.store.PropertyRecord;
import org.neo4j.admin.nioneo.store.RelationshipRecord;
import org.neo4j.admin.nioneo.store.RelationshipStore;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.OptionValueType;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.impl.AbstractApp.OptionContext;

public class Set extends NioneoApp
{

    public Set()
    {
        super();
        this.addValueType( "v", new OptionContext( OptionValueType.MUST,
            "Node set next relationship" ) );
        this.addValueType( "b", new OptionContext( OptionValueType.MUST,
            "Node set next property" ) );
        
        this.addValueType( "q", new OptionContext( OptionValueType.MUST,
            "Relationship set first prev rel" ) );
        this.addValueType( "w", new OptionContext( OptionValueType.MUST,
            "Relationship set first next rel" ) );
        this.addValueType( "e", new OptionContext( OptionValueType.MUST,
            "Relationship set second prev rel" ) );
        this.addValueType( "r", new OptionContext( OptionValueType.MUST,
            "Relationship set second next rel" ) );
        this.addValueType( "t", new OptionContext( OptionValueType.MUST,
            "Relationship set next prop" ) );

        this.addValueType( "u", new OptionContext( OptionValueType.MUST,
            "Property set block value" ) );
        this.addValueType( "pP", new OptionContext( OptionValueType.MUST,
            "Property set prev property" ) );
        this.addValueType( "o", new OptionContext( OptionValueType.MUST,
            "Property set next property" ) );
    }
    
    public String execute( AppCommandParser parser, Session session, Output out )
            throws ShellException
    {
        AbstractRecord record = getServer().getCurrentRecord();
        String response = "set ok";
        if ( record == null )
        {
            response = "Error: No record loaded";
        }
        else if ( record instanceof NodeRecord )
        {
            NodeRecord rec = (NodeRecord) record;
            String nR = checkIfEndHead( parser.options().get( "v" ) );
            String nP = checkIfEndHead( parser.options().get( "b" ) );
            
            if ( nR != null )
            {
                rec.setNextRel( Integer.parseInt( nR ) );
            }
            if ( nP != null )
            {
                rec.setNextProp( Integer.parseInt( nP ) );
            }
        }
        else if ( record instanceof RelationshipRecord )
        {
            RelationshipRecord rec = (RelationshipRecord) record;
            String fpR = checkIfEndHead( parser.options().get( "q" ) );
            String fnR = checkIfEndHead( parser.options().get( "w" ) );
            String spR = checkIfEndHead( parser.options().get( "e" ) );
            String snR = checkIfEndHead( parser.options().get( "r" ) );
            String nP = checkIfEndHead( parser.options().get( "t" ) );
            if ( fpR != null )
            {
                rec.setFirstPrevRel( Integer.parseInt( fpR ) );
            }
            if ( fnR != null )
            {
                rec.setFirstNextRel( Integer.parseInt( fnR ) );
            }
            if ( spR != null )
            {
                rec.setSecondPrevRel( Integer.parseInt( spR ) );
            }
            if ( snR != null )
            {
                rec.setSecondNextRel( Integer.parseInt( snR ) );
            }
            if ( nP != null )
            {
                rec.setNextProp( Integer.parseInt( nP ) );
            }
        }
        else if ( record instanceof PropertyRecord )
        {
            PropertyRecord rec = (PropertyRecord) record;
            String value = checkIfEndHead( parser.options().get( "u" ) );
            String pP = checkIfEndHead( parser.options().get( "i" ) );
            String nP = checkIfEndHead( parser.options().get( "o" ) );
            if ( value != null )
            {
                rec.setPropBlock( Long.parseLong( value ) );
            }
            if ( pP != null )
            {
                rec.setPrevProp( Integer.parseInt( pP ) );
            }
            if ( nP != null )
            {
                rec.setNextProp( Integer.parseInt( nP ) );
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
    
    private String checkIfEndHead( String str )
    {
        if ( str == null )
        {
            return null;
        }
        if ( str.toLowerCase().trim().equals( "end" ) )
        {
            return "-1";
        }
        return str;
    }
}
