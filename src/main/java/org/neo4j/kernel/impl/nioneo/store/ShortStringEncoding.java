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

public enum ShortStringEncoding
{
    EMPTY,
    UTF8,
    LATIN1,
    NUMERICAL,
    UPPER,
    LOWER,
    ALPHANUM,
    EUROPEAN, ;

    public static ShortStringEncoding getEncoding( String string )
    {
        PropertyRecord target = new PropertyRecord( -1 );
        if ( ShortString.encode( string, target ) )
        {
            return getEncoding( target.getPropBlock() );
        }
        else
        {
            return null;
        }
    }

    /**
     * OBS: duplicates the switch statement from
     * {@link ShortString#decode(long)}
     */
    public static ShortStringEncoding getEncoding( long encoded )
    {
        if ( encoded == 0 ) return EMPTY;
        int header = (int) ( encoded >>> 56 );
        switch ( header >>> 4 )
        {
        case 0: // 0b0000 - NUMERICAL 4bit (0-14 chars)
            if ( ( header &= 0x0F ) == 0 ) // 0b0000 0000 - UTF-8 (0-7 bytes)
                return UTF8;
            //$FALL-THROUGH$
        case 1: // 0b0001 - NUMERICAL 4bit (15 chars)
            return NUMERICAL;
        case 2: // 0b0010 - UPPER 5bit (0-11 chars)
        case 3: // 0b0011 - UPPER 5bit (12 chars)
            return UPPER;
        case 4: // 0b0100 - LOWER 5bit (0-11 chars)
        case 5: // 0b0101 - LOWER 5bit (12 chars)
            return LOWER;
        case 6: // 0b0110 - ALPHANUM 6bit (10 chars)
            return ALPHANUM;
        case 7: // 0b0111 - EUROPEAN 7bit (1-8 chars) or LATIN1 8bit (0-7 chars)
            if ( ( header & 0x08 ) != 0 ) // 0b0111 1 - LATIN1 8bit (0-7 chars)
                return LATIN1;
            //$FALL-THROUGH$
        default: // 0b1XXX- EUROPEAN 7bit (9 chars)
            return EUROPEAN;
        }
    }

    public static boolean store( PropertyRecord record, String string )
    {
        if ( ShortString.encode( string, record ) )
        {
            record.setType( PropertyType.SHORT_STRING );
            return true;
        }
        return false;
    }
}
