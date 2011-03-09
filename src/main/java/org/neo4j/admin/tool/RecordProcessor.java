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
package org.neo4j.admin.tool;

import org.neo4j.kernel.impl.nioneo.store.Abstract64BitRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;

public interface RecordProcessor<R extends Abstract64BitRecord>
{
    void process( R record );

    interface NodeProcessor
    {
        void processNode( NodeRecord record );
    }

    interface RelationshipProcessor
    {
        void processRelationship( RelationshipRecord record );
    }

    interface PropertyProcessor
    {
        void processProperty( PropertyRecord record );
    }

    class Factory
    {
        public static RecordProcessor<NodeRecord> nodeProcessor( final NodeProcessor processor )
        {
            return new RecordProcessor<NodeRecord>()
            {
                @Override
                public void process( NodeRecord record )
                {
                    processor.processNode( record );
                }

                @Override
                public String toString()
                {
                    return processor + " for nodes";
                }
            };
        }

        public static RecordProcessor<RelationshipRecord> relationshipProcessor( final RelationshipProcessor processor )
        {
            return new RecordProcessor<RelationshipRecord>()
            {
                @Override
                public void process( RelationshipRecord record )
                {
                    processor.processRelationship( record );
                }

                @Override
                public String toString()
                {
                    return processor + " for relationships";
                }
            };
        }

        public static RecordProcessor<PropertyRecord> propertyProcessor( final PropertyProcessor processor )
        {
            return new RecordProcessor<PropertyRecord>()
            {
                @Override
                public void process( PropertyRecord record )
                {
                    processor.processProperty( record );
                }

                @Override
                public String toString()
                {
                    return processor + " for properties";
                }
            };
        }

        private Factory()
        {
            // cannot be constructed
        }
    }
}
