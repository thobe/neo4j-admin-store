/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.admin.nioneo;

import org.neo4j.kernel.impl.nioneo.store.Abstract64BitRecord;
import org.neo4j.kernel.impl.nioneo.store.AbstractRecord;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyType;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;


public class Util
{
    static String getRecordString( Abstract64BitRecord rec )
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
