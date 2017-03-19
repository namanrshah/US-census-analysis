/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package proj.analysis.uscensus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author namanrs
 */
public class USCensusSecondReducer extends Reducer<Text, Text, Text, Text> {

    double maxPercAdult = 0.0;
    String maxAdultState = "";
    List<Double> avgRoomsList = new ArrayList<>();

    @Override
    protected void reduce(Text key, Iterable<Text> values,
            Context context) throws IOException, InterruptedException {
        String header = String.format("%-10s%-10s%-10s%-10s%-10s%-10s%-10s%-10s%-10s%-10s%-10s%-10s%-20s%-15s", "1.Rent", "1.Owned", "2.M", "2.F", "3.a.M", "3.b.M", "3.c.M", "3.a.F", "3.b.F", "3.c.F", "4.Rural", "4.Urban", "5", "6");
        context.write(new Text("State"), new Text(header));
        //1 to 6th
        for (Text value : values) {
            String valueForState = value.toString();
            String[] answers = valueForState.split(Constants.delim);
            String formattedAnswers = String.format("%-10s%-10s%-10s%-10s%-10s%-10s%-10s%-10s%-10s%-10s%-10s%-10s%-20s%-15s", answers[1], answers[2], answers[3], answers[4], answers[5], answers[6], answers[7], answers[8], answers[9], answers[10], answers[11], answers[12], answers[13], answers[14]);
            int ansLen = answers.length;
            avgRoomsList.add(Double.parseDouble((answers[ansLen - 2])));
            double ageGt85 = Double.parseDouble(answers[ansLen - 1]);
            if (ageGt85 > maxPercAdult) {
                maxPercAdult = ageGt85;
                maxAdultState = answers[0];
            }
            context.write(new Text(answers[0]), new Text(formattedAnswers));
        }
        //7th
        Collections.sort(avgRoomsList);
        int roomListSize = avgRoomsList.size();
        double indexOf95Perc = 95 * roomListSize / 100d;
        long round = Math.round(indexOf95Perc);
        Double roomValFor95thPerc = avgRoomsList.get((int) (round - 1));
        context.write(new Text("7."), new Text(String.format("%.2f", roomValFor95thPerc)));
        //8th
        context.write(new Text("8."), new Text(maxAdultState));
    }
}
