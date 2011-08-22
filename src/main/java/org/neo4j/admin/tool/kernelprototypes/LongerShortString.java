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
package org.neo4j.admin.tool.kernelprototypes;

import java.io.UnsupportedEncodingException;
import java.util.EnumSet;

import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;

/**
 * Supports encoding alphanumerical and <code>SP . - + , ' : / _</code>
 * 
 * (This version assumes 14bytes property block, instead of 8bytes)
 *
 * @author Tobias Ivarsson <tobias.ivarsson@neotechnology.com>
 */
public enum LongerShortString
{
    /**
     * Binary coded decimal with punctuation.
     *
     * <pre>
     *    -0 -1 -2 -3 -4 -5 -6 -7   -8 -9 -A -B -C -D -E -F
     * 0-  0  1  2  3  4  5  6  7    8  9 SP  .  -  +  ,  '
     * </pre>
     */
    NUMERICAL( 1, 4 )
    {
        @Override
        int encTranslate( byte b )
        {
            if ( b >= '0' && b <= '9' ) return b - '0';
            switch ( b )
            {
            // interm.    encoded
            case 0: return 0xA;
            case 2: return 0xB;
            case 3: return 0xC;
            case 6: return 0xD;
            case 7: return 0xE;
            case 8: return 0xF;
            default: throw cannotEncode( b );
            }
        }

        @Override
        int encPunctuation( byte b )
        {
            throw cannotEncode( b );
        }

        @Override
        char decTranslate( byte codePoint )
        {
            if ( codePoint < 10 ) return (char) ( codePoint + '0' );
            return decPunctuation( ( codePoint - 10 + 6 ) );
        }
    },
    /**
     * Binary coded decimal with punctuation.
     *
     * <pre>
     *    -0 -1 -2 -3 -4 -5 -6 -7   -8 -9 -A -B -C -D -E -F
     * 0-  0  1  2  3  4  5  6  7    8  9 SP  -  :  /  +  ,
     * </pre>
     */
    DATE( 2, 4 )
    {
        @Override
        int encTranslate( byte b )
        {
            if ( b >= '0' && b <= '9' ) return b - '0';
            switch ( b )
            {
            case 0: return 0xA;
            case 3: return 0xB;
            case 4: return 0xC;
            case 5: return 0xD;
            case 6: return 0xE;
            case 7: return 0xF;
            default: throw cannotEncode( b );
            }
        }

        @Override
        int encPunctuation( byte b )
        {
            throw cannotEncode( b );
        }

        @Override
        char decTranslate( byte codePoint )
        {
            if ( codePoint < 0xA ) return (char) ( codePoint + '0' );
            switch ( codePoint )
            {
            case 0xA: return ' ';
            case 0xB: return '-';
            case 0xC: return ':';
            case 0xD: return '/';
            case 0xE: return '+';
            default: return ',';
            }
        }
    },
    /**
     * Upper-case characters with punctuation.
     *
     * <pre>
     *    -0 -1 -2 -3 -4 -5 -6 -7   -8 -9 -A -B -C -D -E -F
     * 0- SP  A  B  C  D  E  F  G    H  I  J  K  L  M  N  O
     * 1-  P  Q  R  S  T  U  V  W    X  Y  Z  _  .  -  :  /
     * </pre>
     */
    UPPER( 3, 5 )
    {
        @Override
        int encTranslate( byte b )
        {
            return super.encTranslate( b ) - 0x40;
        }

        @Override
        int encPunctuation( byte b )
        {
            return b == 0 ? 0x40 : b + 0x5a;
        }

        @Override
        char decTranslate( byte codePoint )
        {
            if ( codePoint == 0 ) return ' ';
            if ( codePoint <= 0x1A ) return (char) ( codePoint + 'A' - 1 );
            return decPunctuation( codePoint - 0x1A );
        }
    },
    /**
     * Lower-case characters with punctuation.
     *
     * <pre>
     *    -0 -1 -2 -3 -4 -5 -6 -7   -8 -9 -A -B -C -D -E -F
     * 0- SP  a  b  c  d  e  f  g    h  i  j  k  l  m  n  o
     * 1-  p  q  r  s  t  u  v  w    x  y  z  _  .  -  :  /
     * </pre>
     */
    LOWER( 4, 5 )
    {
        @Override
        int encTranslate( byte b )
        {
            return super.encTranslate( b ) - 0x60;
        }

        @Override
        int encPunctuation( byte b )
        {
            return b == 0 ? 0x60 : b + 0x7a;
        }

        @Override
        char decTranslate( byte codePoint )
        {
            if ( codePoint == 0 ) return ' ';
            if ( codePoint <= 0x1A ) return (char) ( codePoint + 'a' - 1 );
            return decPunctuation( codePoint - 0x1A );
        }
    },
    /**
     * Lower-case characters with punctuation.
     *
     * <pre>
     *    -0 -1 -2 -3 -4 -5 -6 -7   -8 -9 -A -B -C -D -E -F
     * 0-  ,  a  b  c  d  e  f  g    h  i  j  k  l  m  n  o
     * 1-  p  q  r  s  t  u  v  w    x  y  z  _  .  -  +  @
     * </pre>
     */
    EMAIL( 5, 5 )
    {
        @Override
        int encTranslate( byte b )
        {
            return super.encTranslate( b ) - 0x60;
        }

        @Override
        int encPunctuation( byte b )
        {
            int encOffset = 0x60;
            if ( b == 7 ) return encOffset;
            
            int offset = encOffset + 0x1B;
            switch ( b )
            {
            case 1: return 0 + offset;
            case 2: return 1 + offset;
            case 3: return 2 + offset;
            case 6: return 3 + offset;
            case 9: return 4 + offset;
            default: throw cannotEncode( b );
            }
        }

        @Override
        char decTranslate( byte codePoint )
        {
            if ( codePoint == 0 ) return ',';
            if ( codePoint <= 0x1A ) return (char) ( codePoint + 'a' - 1 );
            switch ( codePoint )
            {
            case 0x1E: return '+';
            case 0x1F: return '@';
            default: return decPunctuation( codePoint - 0x1A );
            }
        }
    },
    /**
     * Lower-case characters, digits and punctuation and symbols.
     *
     * <pre>
     *    -0 -1 -2 -3 -4 -5 -6 -7   -8 -9 -A -B -C -D -E -F
     * 0- SP  a  b  c  d  e  f  g    h  i  j  k  l  m  n  o
     * 1-  p  q  r  s  t  u  v  w    x  y  z 
     * 2-  0  1  2  3  4  5  6  7    8  9  _  .  -  :  /  +
     * 3-  ,  '  @  |  ;  *  ?  &    %  #  (  )  $  <  >  =
     * </pre>
     */
    URI( 6, 6 )
    {
        @Override
        int encTranslate( byte b )
        {
            if ( b == 0 ) return 0; // space
            if ( b >= 0x61 && b <= 0x7A ) return b - 0x60; // lower-case letters
            if ( b >= 0x30 && b <= 0x39 ) return b - 0x10; // digits
            if ( b >= 0x1 && b <= 0x16 ) return b + 0x29; // symbols
            throw cannotEncode( b );
        }

        @Override
        int encPunctuation( byte b )
        {
            // Handled by encTranslate
            throw cannotEncode( b );
        }

        @Override
        char decTranslate( byte codePoint )
        {
            if ( codePoint == 0 ) return ' ';
            if ( codePoint <= 0x1A ) return (char) ( codePoint + 'a' - 1 );
            if ( codePoint <= 0x29 ) return (char) (codePoint - 0x20 + '0');
            if ( codePoint <= 0x2E ) return decPunctuation( codePoint - 0x29 );
            return decPunctuation( codePoint - 0x2F + 9);
        }
    },
    /**
     * Alpha-numerical characters space and underscore.
     *
     * <pre>
     *    -0 -1 -2 -3 -4 -5 -6 -7   -8 -9 -A -B -C -D -E -F
     * 0- SP  A  B  C  D  E  F  G    H  I  J  K  L  M  N  O
     * 1-  P  Q  R  S  T  U  V  W    X  Y  Z  0  1  2  3  4
     * 2-  _  a  b  c  d  e  f  g    h  i  j  k  l  m  n  o
     * 3-  p  q  r  s  t  u  v  w    x  y  z  5  6  7  8  9
     * </pre>
     */
    ALPHANUM( 7, 6 )
    {
        @Override
        char decTranslate( byte codePoint )
        {
            return EUROPEAN.decTranslate( (byte) ( codePoint + 0x40 ) );
        }

        @Override
        int encTranslate( byte b )
        {
            // Punctuation is in the same places as European
            if ( b < 0x20 ) return encPunctuation( b ); // Punctuation
            // But the rest is transposed by 0x40
            return EUROPEAN.encTranslate( b ) - 0x40;
        }

        @Override
        int encPunctuation( byte b )
        {
            switch ( b )
            {
            case 0:
                return 0x00; // SPACE
            case 1:
                return 0x20; // UNDERSCORE
            default:
                throw cannotEncode( b );
            }
        }
    },
    /**
     * Alpha-numerical characters space and underscore.
     *
     * <pre>
     *    -0 -1 -2 -3 -4 -5 -6 -7   -8 -9 -A -B -C -D -E -F
     * 0- SP  A  B  C  D  E  F  G    H  I  J  K  L  M  N  O
     * 1-  P  Q  R  S  T  U  V  W    X  Y  Z  _  .  -  :  /
     * 2-  |  a  b  c  d  e  f  g    h  i  j  k  l  m  n  o
     * 3-  p  q  r  s  t  u  v  w    x  y  z  +  ,  '  @  ;
     * </pre>
     */
    ALPHASYM( 8, 6 )
    {
        @Override
        char decTranslate( byte codePoint )
        {
            return EUROPEAN.decTranslate( (byte) ( codePoint + 0x40 ) );
        }

        @Override
        int encTranslate( byte b )
        {
            // Punctuation is in the same places as European
            if ( b < 0x20 ) return encPunctuation( b ); // Punctuation
            // But the rest is transposed by 0x40
            return EUROPEAN.encTranslate( b ) - 0x40;
        }

        @Override
        int encPunctuation( byte b )
        {
            switch ( b )
            {
            case 0: return 0x0;
            case 1: return 0x1B;
            case 2: return 0x1C;
            case 3: return 0x1D;
            case 4: return 0x1E;
            case 5: return 0x1F;
            case 6: return 0x3B;
            case 7: return 0x3C;
            case 8: return 0x3D;
            case 9: return 0x3E;
            case 10: return 0x3F;
            default: throw cannotEncode( b );
            }
        }
    },
    /**
     * The most common European characters (latin-1 but with less punctuation).
     *
     * <pre>
     *    -0 -1 -2 -3 -4 -5 -6 -7   -8 -9 -A -B -C -D -E -F
     * 0-  À  Á  Â  Ã  Ä  Å  Æ  Ç    È  É  Ê  Ë  Ì  Í  Î  Ï
     * 1-  Ð  Ñ  Ò  Ó  Ô  Õ  Ö  .    Ø  Ù  Ú  Û  Ü  Ý  Þ  ß
     * 2-  à  á  â  ã  ä  å  æ  ç    è  é  ê  ë  ì  í  î  ï
     * 3-  ð  ñ  ò  ó  ô  õ  ö  -    ø  ù  ú  û  ü  ý  þ  ÿ
     * 4- SP  A  B  C  D  E  F  G    H  I  J  K  L  M  N  O
     * 5-  P  Q  R  S  T  U  V  W    X  Y  Z  0  1  2  3  4
     * 6-  _  a  b  c  d  e  f  g    h  i  j  k  l  m  n  o
     * 7-  p  q  r  s  t  u  v  w    x  y  z  5  6  7  8  9
     * </pre>
     */
    EUROPEAN( 9, 7 )
    {
        @Override
        char decTranslate( byte codePoint )
        {
            if ( codePoint < 0x40 )
            {
                if ( codePoint == 0x17 ) return '.';
                if ( codePoint == 0x37 ) return '-';
                return (char) ( codePoint + 0xC0 );
            }
            else
            {
                if ( codePoint == 0x40 ) return ' ';
                if ( codePoint == 0x60 ) return '_';
                if ( codePoint >= 0x5B && codePoint < 0x60 ) return (char) ( '0' + codePoint - 0x5B );
                if ( codePoint >= 0x7B && codePoint < 0x80 ) return (char) ( '5' + codePoint - 0x7B );
                return (char) codePoint;
            }
        }

        @Override
        int encPunctuation( byte b )
        {
            switch ( b )
            {
            case 0x00:
                return 0x40; // SPACE
            case 0x01:
                return 0x60; // UNDERSCORE
            case 0x02:
                return 0x17; // DOT
            case 0x03:
                return 0x37; // DASH
            case 0x07:
                // TODO
                return 0;
            default:
                throw cannotEncode( b );
            }
        }
    };
    
    final int encodingHeader;
    final short mask;
    final short step;

    private LongerShortString( int encodingHeader, int step )
    {
        this.encodingHeader = encodingHeader;
        this.mask = (short) Bits.rightOverflowMask( step );
        this.step = (short) step;
    }
    
    int maxLength( int payloadSize )
    {
        return ((payloadSize << 3)-24-4-6)/step;
    }

    final IllegalArgumentException cannotEncode( byte b )
    {
        return new IllegalArgumentException( "Cannot encode as " + this.name() + ": " + b );
    }

    /** Lookup table for decoding punctuation */
    private static final char[] PUNCTUATION = {
        ' ', '_', '.', '-', ':', '/',
        ' ', '.', '-', '+', ',', '\'', '@', '|', ';', '*', '?', '&', '%', '#', '(', ')', '$', '<', '>', '=' };

    final char decPunctuation( int code )
    {
        return PUNCTUATION[code];
    }

    int encTranslate( byte b )
    {
        if ( b < 0 ) return ( 0xFF & b ) - 0xC0; // European chars
        if ( b < 0x20 ) return encPunctuation( b ); // Punctuation
        if ( b >= '0' && b <= '4' ) return 0x5B + b - '0'; // Numbers
        if ( b >= '5' && b <= '9' ) return 0x7B + b - '5'; // Numbers
        return b; // Alphabetical
    }

    abstract int encPunctuation( byte b );

    abstract char decTranslate( byte codePoint );

    /**
     * Encodes a short string.
     *
     * @param string the string to encode.
     * @param target the property record to store the encoded string in
     * @return <code>true</code> if the string could be encoded as a short
     *         string, <code>false</code> if it couldn't.
     */
    /*
     * Intermediate code table
     *    -0 -1 -2 -3 -4 -5 -6 -7   -8 -9 -A -B -C -D -E -F
     * 0- SP  _  .  -  :  /  +  ,    '  @  |  ;  *  ?  &  %
     * 1-  #  (  )  $  <  >  =
     * 2-
     * 3-  0  1  2  3  4  5  6  7    8  9
     * 4-     A  B  C  D  E  F  G    H  I  J  K  L  M  N  O
     * 5-  P  Q  R  S  T  U  V  W    X  Y  Z
     * 6-     a  b  c  d  e  f  g    h  i  j  k  l  m  n  o
     * 7-  p  q  r  s  t  u  v  w    x  y  z
     * 8-
     * 9-
     * A-
     * B-
     * C-  À  Á  Â  Ã  Ä  Å  Æ  Ç    È  É  Ê  Ë  Ì  Í  Î  Ï
     * D-  Ð  Ñ  Ò  Ó  Ô  Õ  Ö       Ø  Ù  Ú  Û  Ü  Ý  Þ  ß
     * E-  à  á  â  ã  ä  å  æ  ç    è  é  ê  ë  ì  í  î  ï
     * F-  ð  ñ  ò  ó  ô  õ  ö       ø  ù  ú  û  ü  ý  þ  ÿ
     */
    public static boolean encode( int keyId, String string, PropertyRecord target, int payloadSize )
    {
        // NUMERICAL can carry most characters, so compare to that
        int stringLength = string.length();
        // We only use 6 bits for storing the string length
        // TODO could be dealt with by having string length zero and go for null bytes,
        // at least for LATIN1 (that's what the ShortString implementation initially did)
        if ( stringLength > NUMERICAL.maxLength( payloadSize ) || stringLength > 63 ) return false; // Not handled by any encoding
        if ( string.equals( "" ) )
        {
            applyOnRecord( keyId, target, 0, 0, new long[1] );
            return true;
        }
        // Keep track of the possible encodings that can be used for the string
        EnumSet<LongerShortString> possible = null;
        // First try encoding using Latin-1
//        int maxBytes = PropertyType.getPayloadSize();
        if ( stringLength <= (payloadSize-3-1-1) )
        {
            if ( encodeLatin1( keyId, string, target, payloadSize ) ) return true;
            // If the string was short enough, but still didn't fit in latin-1
            // we know that no other encoding will work either, remember that
            // so that we can try UTF-8 at the end of this method
            possible = EnumSet.noneOf( LongerShortString.class );
        }
        // Allocate space for the intermediate representation
        // (using the intermediate representation table above)
        byte[] data = new byte[stringLength];
        if ( possible == null )
        {
            possible = EnumSet.allOf( LongerShortString.class );
            for ( LongerShortString possibility : LongerShortString.values() )
            {
                if ( data.length > possibility.maxLength( payloadSize ) ) possible.remove( possibility );
            }
        }
        LOOP: for ( int i = 0; i < data.length && !possible.isEmpty(); i++ )
        {
            char c = string.charAt( i );
            switch ( c )
            {
            case ' ':
                data[i] = 0;
                possible.remove( EMAIL );
                break;
            case '_':
                data[i] = 1;
                possible.removeAll( EnumSet.of( NUMERICAL, DATE ) );
                break;
            case '.':
                data[i] = 2;
                possible.removeAll( EnumSet.of( ALPHANUM, DATE ) );
                break;
            case '-':
                data[i] = 3;
                possible.remove( ALPHANUM );
                break;
            case ':':
                data[i] = 4;
                possible.removeAll( EnumSet.of( ALPHANUM, NUMERICAL, EUROPEAN, EMAIL ) );
                break;
            case '/':
                data[i] = 5;
                possible.removeAll( EnumSet.of( ALPHANUM, NUMERICAL, EUROPEAN, EMAIL ) );
                break;
            case '+':
                data[i] = 6;
                possible.retainAll( EnumSet.of( NUMERICAL, DATE, EMAIL, URI, ALPHASYM ) );
                break;
            case ',':
                data[i] = 7;
                possible.retainAll( EnumSet.of( NUMERICAL, DATE, EMAIL, URI, ALPHASYM ) );
                break;
            case '\'':
                data[i] = 8;
                possible.retainAll( EnumSet.of( NUMERICAL, URI, ALPHASYM ) );
                break;
            case '@':
                data[i] = 9;
                possible.retainAll( EnumSet.of( EMAIL, URI, ALPHASYM ) );
                break;
            case '|':
                data[i] = 0xA;
                possible.retainAll( EnumSet.of( ALPHASYM ) );
                break;
            // These below are all for the URI encoding only (as of yet at least)
            case ';':
                data[i] = 0xB;
                possible.retainAll( EnumSet.of( URI ) );
                break;
            case '*':
                data[i] = 0xC;
                possible.retainAll( EnumSet.of( URI ) );
                break;
            case '?':
                data[i] = 0xD;
                possible.retainAll( EnumSet.of( URI ) );
                break;
            case '&':
                data[i] = 0xE;
                possible.retainAll( EnumSet.of( URI ) );
                break;
            case '%':
                data[i] = 0xF;
                possible.retainAll( EnumSet.of( URI ) );
                break;
            case '#':
                data[i] = 0x10;
                possible.retainAll( EnumSet.of( URI ) );
                break;
            case '(':
                data[i] = 0x11;
                possible.retainAll( EnumSet.of( URI ) );
                break;
            case ')':
                data[i] = 0x12;
                possible.retainAll( EnumSet.of( URI ) );
                break;
            case '$':
                data[i] = 0x13;
                possible.retainAll( EnumSet.of( URI ) );
                break;
            case '<':
                data[i] = 0x14;
                possible.retainAll( EnumSet.of( URI ) );
                break;
            case '>': data[i] = 0x15;
                possible.retainAll( EnumSet.of( URI ) );
                break;
            case '=': data[i] = 0x16;
                possible.retainAll( EnumSet.of( URI ) );
                break;
            // These above are all for the URI encoding only (as of yet at least)
            default:
                if ( ( c >= 'A' && c <= 'Z' ) )
                {
                    possible.removeAll( EnumSet.of( NUMERICAL, DATE, LOWER, EMAIL, URI ) );
                }
                else if ( ( c >= 'a' && c <= 'z' ) )
                {
                    possible.removeAll( EnumSet.of( NUMERICAL, DATE, UPPER ) );
                }
                else if ( ( c >= '0' && c <= '9' ) )
                {
                    possible.removeAll( EnumSet.of( UPPER, LOWER, EMAIL, ALPHASYM ) );
                }
                else if ( c >= 'À' && c <= 'ÿ' && c != 0xD7 && c != 0xF7 )
                {
                    possible.retainAll( EnumSet.of( EUROPEAN ) );
                }
                else
                {
                    possible.clear();
                    break LOOP; // fall back to UTF-8
                }
                data[i] = (byte) c;
            }
        }
        for ( LongerShortString encoding : possible )
        {
            // Will return false if the data is too long for the encoding
            if ( encoding.doEncode( keyId, data, target, payloadSize ) ) return true;
        }
        if ( stringLength <= payloadSize )
        { // We might have a chance with UTF-8 - try it!
            return encodeUTF8( keyId, string, target, payloadSize );
        }
        return false;
    }

    private static void applyOnRecord( int keyId, PropertyRecord target, int encoding, int stringLength, long[] data )
    {
        data[0] |= ((long)keyId << 40);
        data[0] |= ((long)encoding << 36);
        data[0] |= ((long)stringLength << 30);
        target.setPropBlock( data[0] );
//        target.setHeader( (0x2 << 10) | (encoding << 6) | (stringLength) );
    }

    /**
     * Decode a short string represented as a long[]
     *
     * @param data the value to decode to a short string.
     * @return the decoded short string
     */
    public static String decode( PropertyRecord record )
    {
        Bits bits = new Bits( new long[] {record.getPropBlock()} );
        long firstLong = bits.getLong( 0xFFFFFFFFFFFFFFFFL );
        if ( firstLong == 0 ) return "";
        // [kkkk,kkkk][kkkk,kkkk][kkkk,kkkk][eeee,llll][ll  ,    ]
        int encoding = (int)((firstLong & 0xF000000000L) >>> 36);
        int stringLength = (int)((firstLong & 0xFC0000000L) >>> 30);
        
        LongerShortString table;
        switch ( encoding )
        {
        case 0: return decodeUTF8( bits, stringLength );
        case 1: table = NUMERICAL; break;
        case 2: table = DATE; break;
        case 3: table = UPPER; break;
        case 4: table = LOWER; break;
        case 5: table = EMAIL; break;
        case 6: table = URI; break;
        case 7: table = ALPHANUM; break;
        case 8: table = ALPHASYM; break;
        case 9: table = EUROPEAN; break;
        case 10: return decodeLatin1( bits, stringLength );
        default: throw new IllegalArgumentException( "Invalid encoding '" + encoding + "'" );
        }
        char[] result = new char[stringLength];
        // encode shifts in the bytes with the first char at the MSB, therefore
        // we must "unshift" in the reverse order
        for ( int i = result.length - 1; i >= 0; i-- )
        {
            byte codePoint = bits.getByte( (byte)table.mask );
            result[i] = table.decTranslate( codePoint );
            bits.shiftRight( table.step );
        }
        return new String( result );
    }
    
    private static Bits newBits( int payloadSize )
    {
        return new Bits( payloadSize );
    }

    private static boolean encodeLatin1( int keyId, String string, PropertyRecord target, int payloadSize )
    { // see doEncode
        Bits bits = newBits( payloadSize );
        for ( int i = 0; i < string.length(); i++ )
        {
            if ( i > 0 ) bits.shiftLeft( 8 );
            char c = string.charAt( i );
            if ( c < 0 || c >= 256 ) return false;
            bits.or( c, 0xFF );
        }
        applyOnRecord( keyId, target, 10, string.length(), bits.getLongs() );
        return true;
    }

    private static boolean encodeUTF8( int keyId, String string, PropertyRecord target, int payloadSize )
    {
        try
        {
            byte[] bytes = string.getBytes( "UTF-8" );
            if ( bytes.length > payloadSize ) return false;
            Bits bits = newBits( payloadSize );
            for ( int i = 0; i < bytes.length; i++ )
            {
                if ( i > 0 ) bits.shiftLeft( 8 );
                bits.or( bytes[i] );
            }
            applyOnRecord( keyId, target, 0, bytes.length, bits.getLongs() );
            return true;
        }
        catch ( UnsupportedEncodingException e )
        {
            throw new IllegalStateException( "All JVMs must support UTF-8", e );
        }
    }

    private boolean doEncode( int keyId, byte[] data, PropertyRecord target, int payloadSize )
    {
        if ( data.length > maxLength( payloadSize ) ) return false;
        Bits bits = newBits( payloadSize );
        for ( int i = 0; i < data.length; i++ )
        {
            if ( i != 0 ) bits.shiftLeft( step );
            int encodedChar = encTranslate( data[i] );
            bits.or( encodedChar, 0xFF );
        }
        applyOnRecord( keyId, target, encodingHeader, data.length, bits.getLongs() );
        return true;
    }

    private static String decodeLatin1( Bits bits, int stringLength )
    { // see decode
        char[] result = new char[stringLength];
        for ( int i = result.length - 1; i >= 0; i-- )
        {
            result[i] = (char) bits.getInt( 0xFF );
            bits.shiftRight( 8 );
        }
        return new String( result );
    }

    private static String decodeUTF8( Bits bits, int stringLength )
    {
        byte[] result = new byte[stringLength];
        for ( int i = stringLength-1; i >= 0; i-- )
        {
            result[i] = bits.getByte( (byte)0xFF );
            bits.shiftRight( 8 );
        }
        try
        {
            return new String( result, "UTF-8" );
        }
        catch ( UnsupportedEncodingException e )
        {
            throw new IllegalStateException( "All JVMs must support UTF-8", e );
        }
    }
}
