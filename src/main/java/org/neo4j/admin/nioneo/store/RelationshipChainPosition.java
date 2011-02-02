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
package org.neo4j.admin.nioneo.store;

public class RelationshipChainPosition
{
    private int nextRecord;
    
    public RelationshipChainPosition( int startRecord )
    {
        nextRecord = startRecord;
    }
    
    public int getNextRecord()
    {
        return nextRecord;
    }
    
    public void setNextRecord( int record )
    {
        nextRecord = record;
    }

    public boolean hasMore()
    {
        return nextRecord != Record.NO_NEXT_RELATIONSHIP.intValue();
    }
}
