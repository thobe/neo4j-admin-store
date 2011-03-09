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
package org.neo4j.kernel.impl.nioneo.store;

public interface Filter<R extends Abstract64BitRecord>
{
    boolean accept( R record );

    Filter<Abstract64BitRecord> IN_USE = new Filter<Abstract64BitRecord>()
    {
        @Override
        public boolean accept( Abstract64BitRecord record )
        {
            return record.inUse();
        }
    };

    interface NodeFilter
    {
        boolean acceptNode( NodeRecord record );
    }

    interface RelationshipFilter
    {
        boolean acceptRelationship( RelationshipRecord record );
    }

    interface PropertyFilter
    {
        boolean acceptProperty( PropertyRecord record );
    }

    class Factory
    {
        public static Filter<NodeRecord> nodeFilter( final NodeFilter filter )
        {
            return new Filter<NodeRecord>()
            {
                @Override
                public boolean accept( NodeRecord record )
                {
                    return filter.acceptNode( record );
                }
            };
        }

        public static Filter<RelationshipRecord> relationshipFilter( final RelationshipFilter filter )
        {
            return new Filter<RelationshipRecord>()
            {
                @Override
                public boolean accept( RelationshipRecord record )
                {
                    return filter.acceptRelationship( record );
                }
            };
        }

        public static Filter<PropertyRecord> propertyFilter( final PropertyFilter filter )
        {
            return new Filter<PropertyRecord>()
            {
                @Override
                public boolean accept( PropertyRecord record )
                {
                    return filter.acceptProperty( record );
                }
            };
        }

        private Factory()
        {
            // cannot be constructed
        }
    }
}
