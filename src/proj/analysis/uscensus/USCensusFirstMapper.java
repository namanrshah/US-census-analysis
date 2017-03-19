/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package proj.analysis.uscensus;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author namanrs
 */
public class USCensusFirstMapper extends Mapper<LongWritable, Text, Text, MapperOPValue> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String segment = value.toString();
        String summaryLevel = segment.substring(10, 13);
        if (summaryLevel.equals("100")) {
            MapperOPValue valueToWrite = new MapperOPValue();
            String state = segment.substring(8, 10);
            valueToWrite.setState(new Text(state));
            int segmentNo = Integer.parseInt(segment.substring(24, 28));
            valueToWrite.setSegmentNo(new IntWritable(segmentNo));
            if (segmentNo == 1) {
                //2nd
                long maleNeverMarried = Long.parseLong(segment.substring(Constants.Index.maleNeverMarried, Constants.Index.maleNeverMarried + 9));
                long femaleNeverMarried = Long.parseLong(segment.substring(Constants.Index.femaleNeverMarried, Constants.Index.femaleNeverMarried + 9));

                valueToWrite.setMaleNeveMarried(new LongWritable(maleNeverMarried));
                valueToWrite.setFemaleNeverMarried(new LongWritable(femaleNeverMarried));
                valueToWrite.setPopulation(new LongWritable(Long.parseLong(segment.substring(Constants.Index.population, Constants.Index.population + 9))));
                //3rd
                long totMaleAge = 0l;
                long totFemaleAge = 0l;

                long maleLt18 = 0;
                long maleBt19and29 = 0;
                long maleBt30and39 = 0;

                long femaleLt18 = 0;
                long femaleBt19and29 = 0;
                long femaleBt30and39 = 0;
                //for male
                for (int i = 1; i <= 31; i++) {
                    int indexToread = Constants.Index.malelt1 + ((i - 1) * 9);
                    long val = Long.parseLong(segment.substring(indexToread, indexToread + 9));
                    totMaleAge += val;
                    if (i <= 13) {
                        maleLt18 += val;
                    } else if (i <= 18) {
                        maleBt19and29 += val;
                    } else if (i <= 20) {
                        maleBt30and39 += val;
                    }
                }

                valueToWrite.setMaleLt18(new LongWritable(maleLt18));
                valueToWrite.setMaleBt19and29(new LongWritable(maleBt19and29));
                valueToWrite.setMaleBt30and39(new LongWritable(maleBt30and39));
                valueToWrite.setTotMaleAge(new LongWritable(totMaleAge));
                //for female
                for (int i = 1; i <= 31; i++) {
                    int indexToread = Constants.Index.femalelt1 + ((i - 1) * 9);
                    long val = Long.parseLong(segment.substring(indexToread, indexToread + 9));
                    totFemaleAge += val;
                    if (i <= 13) {
                        femaleLt18 += val;
                    } else if (i <= 18) {
                        femaleBt19and29 += val;
                    } else if (i <= 20) {
                        femaleBt30and39 += val;
                    }
                }

                valueToWrite.setFemaleLt18(new LongWritable(femaleLt18));
                valueToWrite.setFemaleBt19and29(new LongWritable(femaleBt19and29));
                valueToWrite.setFemaleBt30and39(new LongWritable(femaleBt30and39));
                valueToWrite.setTotFemaleAge(new LongWritable(totFemaleAge));

                //8th
                long totPersonForElder = 0;
                for (int i = 1; i <= 31; i++) {
                    int indexToread = Constants.Index.age + ((i - 1) * 9);
                    long val = Long.parseLong(segment.substring(indexToread, indexToread + 9));
                    totPersonForElder += val;
                    if (i == 31) {
                        valueToWrite.setAgegt85(new LongWritable(val));
                    }
                }
                valueToWrite.setTotPplAge(new LongWritable(totPersonForElder));
//                context.write(new Text(state + Integer.toString(valueToWrite.getSegmentNo().get())), valueToWrite);
            } else if (segmentNo == 2) {
                //1st
                valueToWrite.setTotOwned(new LongWritable(Long.parseLong(segment.substring(Constants.Index.totOwned, Constants.Index.totOwned + 9))));
                valueToWrite.setTotRented(new LongWritable(Long.parseLong(segment.substring(Constants.Index.totRented, Constants.Index.totRented + 9))));

                //4th
                long urbanHouses = 0;
                urbanHouses += Long.parseLong(segment.substring(Constants.Index.insideUrbanized, Constants.Index.insideUrbanized + 9));
                urbanHouses += Long.parseLong(segment.substring(Constants.Index.outsideUrbanized, Constants.Index.outsideUrbanized + 9));
                long rural = Long.parseLong(segment.substring(Constants.Index.rural, Constants.Index.rural + 9));
                long totalHH = urbanHouses + rural;
                totalHH += Long.parseLong(segment.substring(Constants.Index.notDefined, Constants.Index.notDefined + 9));
                valueToWrite.setTotUrbanHH(new LongWritable(urbanHouses));
                valueToWrite.setTotRuralHH(new LongWritable(rural));
                valueToWrite.setTotHH(new LongWritable(totalHH));

                //5th
                String ownOccList = "";
                for (int i = 1; i <= 20; i++) {
                    int indexToread = Constants.Index.ownOcclt15000 + ((i - 1) * 9);
                    Long val = Long.parseLong(segment.substring(indexToread, indexToread + 9));
                    if (i != 20) {
                        ownOccList += val + Constants.delim;
                    } else {
                        ownOccList += val;
                    }
                }
//                valueToWrite.setOwnOccListSize(new IntWritable(ownOccList.size()));
                valueToWrite.setOwnOccList(new Text(ownOccList));
                //6th
                String rentList = new String();
                for (int i = 1; i <= 16; i++) {
                    int indexToread = Constants.Index.rentlt100 + ((i - 1) * 9);
                    Long val = Long.parseLong(segment.substring(indexToread, indexToread + 9));
                    if (i != 16) {
                        rentList += val + Constants.delim;
                    } else {
                        rentList += val;
                    }
                }
//                valueToWrite.setRentListSize(new IntWritable(rentList.size()));
                valueToWrite.setRentList(new Text(rentList));
                //7th
                String roomList = new String();
                for (int i = 1; i <= 9; i++) {
                    int indexToread = Constants.Index.room1 + ((i - 1) * 9);
                    Long val = Long.parseLong(segment.substring(indexToread, indexToread + 9));
                    if (i != 9) {
                        roomList += val + Constants.delim;
                    } else {
                        roomList += val;
                    }
                }
//                valueToWrite.setRoomListSize(new IntWritable(roomList.size()));
                valueToWrite.setRoomList(new Text(roomList));
//                context.write(new Text(state + Integer.toString(valueToWrite.getSegmentNo().get())), valueToWrite);
            }
            context.write(new Text(state), valueToWrite);
        }
    }
}
