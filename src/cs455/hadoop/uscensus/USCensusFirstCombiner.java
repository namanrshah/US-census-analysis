package cs455.hadoop.uscensus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author namanrs
 */
public class USCensusFirstCombiner extends Reducer<Text, MapperOPValue, Text, MapperOPValue> {

    @Override
    protected void reduce(Text key, Iterable<MapperOPValue> values,
            Context context) throws IOException, InterruptedException {

        //1st//
        long totRented = 0;
        long totOwned = 0;
        //2nd//
        long maleNeveMarried = 0;
        long femaleNeverMarried = 0;
        long population = 0;
        //3rd//
        long maleLt18 = 0;
        long maleBt19and29 = 0;
        long maleBt30and39 = 0;
        long totMaleAge = 0;

        long femaleLt18 = 0;
        long femaleBt19and29 = 0;
        long femaleBt30and39 = 0;
        long totFemaleAge = 0;
        //4th//
        long totUrbanHH = 0;
        long totRuralHH = 0;
        long totHH = 0;
        //5th//
        List<Long> ownOccListFinal = new ArrayList<>();
        //6th//
        List<Long> rentListFinal = new ArrayList<>();
        //7th//
        List<Long> roomListFinal = new ArrayList<>();
        //8th
        long agegt85 = 0;
        long totPplAge = 0;
        //
        for (MapperOPValue value : values) {
            IntWritable segmentNo = value.getSegmentNo();
            if (segmentNo.get() == 1) {
                //2nd
                maleNeveMarried += value.getMaleNeveMarried().get();
                femaleNeverMarried += value.getFemaleNeverMarried().get();
                population += value.getPopulation().get();
                //3rd
                maleLt18 += value.getMaleLt18().get();
                maleBt19and29 += value.getMaleBt19and29().get();
                maleBt30and39 += value.getMaleBt30and39().get();
                totMaleAge += value.getTotMaleAge().get();

                femaleLt18 += value.getFemaleLt18().get();
                femaleBt19and29 += value.getFemaleBt19and29().get();
                femaleBt30and39 += value.getFemaleBt30and39().get();
                totFemaleAge += value.getTotFemaleAge().get();
                //8th
                agegt85 += value.getAgegt85().get();
                totPplAge += value.getTotPplAge().get();
            } else if (segmentNo.get() == 2) {
                //1st
                totRented += value.getTotRented().get();
                totOwned += value.getTotOwned().get();
                //4th
                totUrbanHH += value.getTotUrbanHH().get();
                totRuralHH += value.getTotRuralHH().get();
                totHH += value.getTotHH().get();
                //5th//
                Text ownOccList = value.getOwnOccList();
                String ownOccListStr = ownOccList.toString();
                String[] ownOccListSplitted = ownOccListStr.split(Constants.delim);
                int ownOccListLen = ownOccListSplitted.length;
                if (ownOccListFinal.isEmpty()) {
                    for (int i = 0; i < ownOccListLen; i++) {
                        ownOccListFinal.add(Long.parseLong(ownOccListSplitted[i]));
                    }
                } else {
                    for (int i = 0; i < ownOccListLen; i++) {
                        Long finalLong = ownOccListFinal.get(i);
                        finalLong += Long.parseLong(ownOccListSplitted[i]);
                        ownOccListFinal.set(i, finalLong);
                    }
                }
                //6th//
                Text rentList = value.getRentList();
//                int rentListsize = rentList.size();
                String rentListStr = rentList.toString();
                String[] rentListSplitted = rentListStr.split(Constants.delim);
                int rentListLen = rentListSplitted.length;
                if (rentListFinal.isEmpty()) {
                    for (int i = 0; i < rentListLen; i++) {
                        rentListFinal.add(Long.parseLong(rentListSplitted[i]));
                    }
                } else {
                    for (int i = 0; i < rentListLen; i++) {
                        Long finalLong = rentListFinal.get(i);
                        finalLong += Long.parseLong(rentListSplitted[i]);
                        rentListFinal.set(i, finalLong);
                    }
                }
                //7th//
                Text roomList = value.getRoomList();
                String roomListStr = roomList.toString();
                String[] roomListSplitted = roomListStr.split(Constants.delim);
                int roomListLen = roomListSplitted.length;
                if (roomListFinal.isEmpty()) {
                    for (int i = 0; i < roomListLen; i++) {
                        roomListFinal.add(Long.parseLong(roomListSplitted[i]));
                    }
                } else {
                    for (int i = 0; i < roomListLen; i++) {
                        Long finalLong = roomListFinal.get(i);
                        finalLong += Long.parseLong(roomListSplitted[i]);
                        roomListFinal.set(i, finalLong);
                    }
                }
            }
        }
        MapperOPValue valueToWrite = new MapperOPValue();
        valueToWrite.setAgegt85(new LongWritable(agegt85));
        valueToWrite.setFemaleBt19and29(new LongWritable(femaleBt19and29));
        valueToWrite.setFemaleBt30and39(new LongWritable(femaleBt30and39));
        valueToWrite.setFemaleLt18(new LongWritable(femaleLt18));
        valueToWrite.setFemaleNeverMarried(new LongWritable(femaleNeverMarried));
        valueToWrite.setMaleBt19and29(new LongWritable(maleBt19and29));
        valueToWrite.setMaleBt30and39(new LongWritable(maleBt30and39));
        valueToWrite.setMaleLt18(new LongWritable(maleLt18));
        valueToWrite.setMaleNeveMarried(new LongWritable(maleNeveMarried));
        String ownOccList = "";
        if (!ownOccListFinal.isEmpty()) {
            for (int i = 0; i < 20; i++) {
                if (i != 19) {
                    ownOccList += ownOccListFinal.get(i) + Constants.delim;
                } else {
                    ownOccList += ownOccListFinal.get(i);
                }
            }
        } else {
            ownOccList = "0";
        }
        valueToWrite.setOwnOccList(new Text(ownOccList));
        valueToWrite.setPopulation(new LongWritable(population));
        String rentList = "";
        if (!rentListFinal.isEmpty()) {
            for (int i = 0; i < 16; i++) {
                if (i != 15) {
                    rentList += rentListFinal.get(i) + Constants.delim;
                } else {
                    rentList += rentListFinal.get(i);
                }
            }
        } else {
            rentList = "0";
        }
        valueToWrite.setRentList(new Text(rentList));
        String roomList = "";
        if (!roomListFinal.isEmpty()) {
            for (int i = 0; i < 9; i++) {
                if (i != 8) {
                    roomList += roomListFinal.get(i) + Constants.delim;
                } else {
                    roomList += roomListFinal.get(i);
                }
            }
        } else {
            roomList = "0";
        }
        valueToWrite.setRoomList(new Text(roomList));
        valueToWrite.setSegmentNo(new IntWritable(0));
        valueToWrite.setState(key);
        valueToWrite.setTotFemaleAge(new LongWritable(totFemaleAge));
        valueToWrite.setTotHH(new LongWritable(totHH));
        valueToWrite.setTotMaleAge(new LongWritable(totMaleAge));
        valueToWrite.setTotOwned(new LongWritable(totOwned));
        valueToWrite.setTotPplAge(new LongWritable(totPplAge));
        valueToWrite.setTotRented(new LongWritable(totRented));
        valueToWrite.setTotRuralHH(new LongWritable(totRuralHH));
        valueToWrite.setTotUrbanHH(new LongWritable(totUrbanHH));
        context.write(key, valueToWrite);
    }
}
