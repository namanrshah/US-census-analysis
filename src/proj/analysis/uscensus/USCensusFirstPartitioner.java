/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package proj.analysis.uscensus;

import java.util.ArrayList;
import java.util.Arrays;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 *
 * @author namanrs
 */
public class USCensusFirstPartitioner extends Partitioner<Text, MapperOPValue> {

    public static String[] states = {"AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VI", "VT", "VA", "WA", "WV", "WI", "WY"};
    public static ArrayList<String> statesName = new ArrayList<String>(Arrays.asList(states));

    @Override
    public int getPartition(Text key, MapperOPValue mapperOPValue, int numberOfReducers) {
        return statesName.indexOf(key.toString());
    }
}
