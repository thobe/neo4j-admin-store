package org.neo4j.admin.nioneo;

import org.neo4j.admin.nioneo.store.AbstractRecord;
import org.neo4j.admin.nioneo.store.DynamicRecord;
import org.neo4j.admin.nioneo.store.NodeRecord;
import org.neo4j.admin.nioneo.store.PropertyRecord;
import org.neo4j.admin.nioneo.store.PropertyType;
import org.neo4j.admin.nioneo.store.RelationshipRecord;


public class Util
{
    static String getRecordString( AbstractRecord rec )
    {
        StringBuffer buf = new StringBuffer();
        if ( rec instanceof NodeRecord )
        {
            NodeRecord record = (NodeRecord) rec;
            buf.append(  "node record #" );
            buf.append( record.getId() ).append( " [" );
            buf.append( record.inUse() );
            buf.append( "|nR " ).append( record.getNextRel() );
            buf.append( "|nP " ).append( record.getNextProp() );
            buf.append( "]" );
        }
        else if ( rec instanceof RelationshipRecord )
        {
            RelationshipRecord record = (RelationshipRecord) rec;
            buf.append( "rel record #" );
            buf.append( record.getId() ).append( " [" );
            buf.append( record.inUse() );
            buf.append( "|fN " ).append( record.getFirstNode() );
            buf.append( "|sN " ).append( record.getSecondNode() );
            buf.append( "|t " ).append( record.getType() );
            buf.append( "|fpR " ).append( record.getFirstPrevRel() );
            buf.append( "|fnR " ).append( record.getFirstNextRel() );
            buf.append( "|spR " ).append( record.getSecondPrevRel() );
            buf.append( "|snR " ).append( record.getSecondNextRel() );
            buf.append( "|nP " ).append( record.getNextProp() );
            buf.append( "]" );
        }
        else if ( rec instanceof PropertyRecord )
        {
            PropertyRecord record = (PropertyRecord) rec;
            buf.append( "prop record #" );
            buf.append( record.getId() ).append( " [" );
            buf.append( record.inUse() );
            buf.append( "|type " ).append( record.getType() );
            buf.append( "|key " ).append( record.getKeyIndexId() );
            buf.append( "|value " ).append( record.getPropBlock() );
            buf.append( "|pP " ).append( record.getPrevProp() );
            buf.append( "|nP " ).append( record.getNextProp() );
            buf.append( "]" );        
        }
        else if ( rec instanceof DynamicRecord )
        {
            DynamicRecord record = (DynamicRecord) rec;
            if ( record.getType() == PropertyType.STRING.intValue() )
            {
                buf.append( "string record #" );
            }
            else if ( record.getType() == PropertyType.ARRAY.intValue() )
            {
                buf.append( "array record #" );
            }
            else
            {
                buf.append( "dynamic record #" );
            }
            buf.append( record.getId() ).append( " [" );
            buf.append( record.inUse() );
            buf.append( "|pB " ).append( record.getPrevBlock() );
            buf.append( "|l " ).append( record.getLength() );
            buf.append( "|nB " ).append( record.getNextBlock() );
            buf.append( "|" );
            byte data[] = record.getData();
            for ( int i = 0; i < 8; i++ )
            {
                buf.append( " " ).append( data[i] );
            }
            buf.append( "...]" );
        }
        else
        {
            return "unkown record " + rec;
        }
        return buf.toString();
    }
}
