package hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.spark.api.java.function.PairFunction;

import entity.covid.CovidCounty;
import scala.Tuple2;

public class CovidPairFunction implements PairFunction<CovidCounty, ImmutableBytesWritable, Put>{

	private static final long serialVersionUID = 1L;

	@Override
	public Tuple2<ImmutableBytesWritable, Put> call(CovidCounty covidCounty) throws Exception {
		Put put = CovidHbaseTable.createPutObject(covidCounty);
		return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);
	}

}
