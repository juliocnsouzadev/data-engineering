package br.com.juliocnsouza.spark_essentials;

import br.com.juliocnsouza.spark_essentials.conf.Cores;
import br.com.juliocnsouza.spark_essentials.conf.MasterType;
import br.com.juliocnsouza.spark_essentials.conf.SparkConfigBuilder;
import java.util.stream.StreamSupport;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 *
 * @author julio
 */
public class Main {

    public static void main( String[] args ) {
        Logger.getLogger( "org.apache" ).setLevel( Level.WARN );
        dummyExample();
    }

    private static void dummyExample() {
        final SparkConf conf = SparkConfigBuilder.getInstance( "Dummy" , MasterType.LOCAL , Cores.ALL );
        JavaSparkContext sc = new JavaSparkContext( conf );
        final JavaRDD<Dummy.Model> rdd = sc.parallelize( Dummy.Datasets.models( 1000 ) );

        final Double sum = rdd.map( item -> item.getDoubleValue() ).reduce( ( a , b ) -> a + b );
        System.out.println( "\n********************\nThe sum is " + sum );

        rdd.groupBy( item -> item.isBooleanValue() ).collect().forEach( item -> {
            System.out.println(
                    "Bool Value: " + item._1 + " - Counts: " + StreamSupport.stream( item._2.spliterator() , false ).count() );
        } );

        sc.close();
    }

}
