package br.com.juliocnsouza.spark_essentials;

import br.com.juliocnsouza.spark_essentials.util.Hasher;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;

/**
 *
 * @author julio
 */
public class Dummy {

    private final static Random RANDOM = new Random();

    public static String genRandomStr( int length ) {
        byte[] array = new byte[ length ];
        RANDOM.nextBytes( array );
        String generatedString = new String( array , Charset.forName( "UTF-8" ) );
        return generatedString;
    }

    public static class Model implements Serializable {

        public static Model getAnyInstance() {
            return new Model( genRandomStr( 7 ) , RANDOM.nextInt() , RANDOM.nextDouble() , RANDOM.nextBoolean() );
        }

        private final String id;
        private final String strValue;
        private final int intValue;
        private final double doubleValue;
        private final boolean booleanValue;

        public Model( String strValue , int intValue , double doubleValue , boolean booleanValue ) {
            this.strValue = strValue;
            this.intValue = intValue;
            this.doubleValue = doubleValue;
            this.booleanValue = booleanValue;
            this.id = Hasher.getHash( this.toString() );
        }

        @Override
        public boolean equals( Object obj ) {
            if ( this == obj ) {
                return true;
            }
            if ( obj == null ) {
                return false;
            }
            if ( getClass() != obj.getClass() ) {
                return false;
            }
            final Model other = ( Model ) obj;
            if ( !Objects.equals( this.id , other.id ) ) {
                return false;
            }
            return true;
        }

        public double getDoubleValue() {
            return doubleValue;
        }

        public String getId() {
            return id;
        }

        public int getIntValue() {
            return intValue;
        }

        public String getStrValue() {
            return strValue;
        }

        @Override
        public int hashCode() {
            int hash = 3;
            hash = 97 * hash + Objects.hashCode( this.id );
            return hash;
        }

        public boolean isBooleanValue() {
            return booleanValue;
        }

        @Override
        public String toString() {
            return "Model{" + "strValue=" + strValue + ", intValue=" + intValue + ", doubleValue=" + doubleValue + ", booleanValue=" + booleanValue + '}';
        }

    }

    public static class Datasets {

        public static List<Double> doubles( int size ) {
            final List<Double> list = new ArrayList<Double>();
            for ( int i = 0 ; i < size ; i++ ) {
                list.add( RANDOM.nextDouble() );
            }
            return list;
        }

        public static List<Model> models( int size ) {
            final List<Model> list = new ArrayList<Model>();
            for ( int i = 0 ; i < size ; i++ ) {
                list.add( Model.getAnyInstance() );
            }
            return list;
        }
    }

}
