package proj.analysis.uscensus;

import java.io.IOException;
import java.text.NumberFormat;
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
public class USCensusFirstReducer extends Reducer<Text, MapperOPValue, Text, Text> {

    static List<String> ownOccList = new ArrayList<>();
    static List<String> rentedList = new ArrayList<>();

    static {
        ownOccList.add("Less Than $15,000");
        ownOccList.add("$15,000 - $19,999");
        ownOccList.add("$20,000 - $24,999");
        ownOccList.add("$25,000 - $29,999");
        ownOccList.add("$30,000 - $34,999");
        ownOccList.add("$35,000 - $39,999");
        ownOccList.add("$40,000 - $44,999");
        ownOccList.add("$45,000 - $49,999");
        ownOccList.add("$50,000 - $59,999");
        ownOccList.add("$60,000 - $74,999");
        ownOccList.add("$75,000 - $99,999");
        ownOccList.add("$100,000 - $124,999");
        ownOccList.add("$125,000 - $149,999");
        ownOccList.add("$150,000 - $174,999");
        ownOccList.add("$175,000 - $199,999");
        ownOccList.add("$200,000 - $249,999");
        ownOccList.add("$250,000 - $299,999");
        ownOccList.add("$300,000 - $399,999");
        ownOccList.add("$400,000 - $499,999");
        ownOccList.add("$500,000 or more");
        //Rent list
        rentedList.add("Less than $100");
        rentedList.add("$100 - $149");
        rentedList.add("$150 - $199");
        rentedList.add("$200 - $249");
        rentedList.add("$250 - $299");
        rentedList.add("$300 - $349");
        rentedList.add("$350 - $399");
        rentedList.add("$400 - $449");
        rentedList.add("$450 - $499");
        rentedList.add("$500 - $549");
        rentedList.add("$550 - $599");
        rentedList.add("$600 - $649");
        rentedList.add("$650 - $699");
        rentedList.add("$700 - $749");
        rentedList.add("$750 - $999");
        rentedList.add("$1000 or more");
    }

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
        for (MapperOPValue value : values) {
            IntWritable segmentNo = value.getSegmentNo();
//            if (segmentNo.get() == 1) {
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
//            } else if (segmentNo.get() == 2) {
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
//            }
        }
        Text keyToWrite = new Text();
        Text valToWrite = new Text();
        //1st
        keyToWrite.set("1. Rented VS Owned");
        valToWrite.set("");
//        context.write(keyToWrite, valToWrite);
        double percRented = totRented * 100d / (totRented + totOwned);
        keyToWrite.set("Rented = ");
        valToWrite.set(NumberFormat.getInstance().format(percRented));
//        context.write(keyToWrite, valToWrite);
        double percOwned = totOwned * 100d / (totRented + totOwned);
        keyToWrite.set("Owned = ");
        valToWrite.set(NumberFormat.getInstance().format(percOwned));
//        context.write(keyToWrite, valToWrite);
        //2nd
        keyToWrite.set("2. Never married");
        valToWrite.set("");
//        context.write(keyToWrite, valToWrite);
        double percmaleNeverMarried = maleNeveMarried * 100d / population;
        keyToWrite.set("Male = ");
        valToWrite.set(NumberFormat.getInstance().format(percmaleNeverMarried));
//        context.write(keyToWrite, valToWrite);
        double percfemaleNeverMarried = femaleNeverMarried * 100d / population;
        keyToWrite.set("Female = ");
        valToWrite.set(NumberFormat.getInstance().format(percfemaleNeverMarried));
//        context.write(keyToWrite, valToWrite);
        //3rd
        keyToWrite.set("3. Age distribution based on gender");
        valToWrite.set("");
//        context.write(keyToWrite, valToWrite);
        //male
        keyToWrite.set("Male");
        valToWrite.set("");
//        context.write(keyToWrite, valToWrite);
        double percMaleLt18 = maleLt18 * 100d / totMaleAge;
        keyToWrite.set("Less than 18");
        valToWrite.set(NumberFormat.getInstance().format(percMaleLt18));
//        context.write(keyToWrite, valToWrite);
        double percMalebt19and29 = maleBt19and29 * 100d / totMaleAge;
        keyToWrite.set("Between 19 and 29");
        valToWrite.set(NumberFormat.getInstance().format(percMalebt19and29));
//        context.write(keyToWrite, valToWrite);
        double percMalebt30and39 = maleBt30and39 * 100d / totMaleAge;
        keyToWrite.set("Between 30 and 39");
        valToWrite.set(NumberFormat.getInstance().format(percMalebt30and39));
//        context.write(keyToWrite, valToWrite);
        //female
        keyToWrite.set("Female");
        valToWrite.set("");
//        context.write(keyToWrite, valToWrite);
        double percFemaleLt18 = femaleLt18 * 100d / totFemaleAge;
        keyToWrite.set("Less than 18");
        valToWrite.set(NumberFormat.getInstance().format(percFemaleLt18));
//        context.write(keyToWrite, valToWrite);
        double percFemalebt19and29 = femaleBt19and29 * 100d / totFemaleAge;
        keyToWrite.set("Between 19 and 29");
        valToWrite.set(NumberFormat.getInstance().format(percFemalebt19and29));
//        context.write(keyToWrite, valToWrite);
        double percFemalebt30and39 = femaleBt30and39 * 100d / totFemaleAge;
        keyToWrite.set("Between 30 and 39");
        valToWrite.set(NumberFormat.getInstance().format(percFemalebt30and39));
//        context.write(keyToWrite, valToWrite);
        //4th
        keyToWrite.set("4. Household distribution");
        valToWrite.set("");
//        context.write(keyToWrite, valToWrite);
        double percRural = totRuralHH * 100d / totHH;
        keyToWrite.set("Rural");
        valToWrite.set(NumberFormat.getInstance().format(percRural));
//        context.write(keyToWrite, valToWrite);
        double percUrban = totUrbanHH * 100d / totHH;
        keyToWrite.set("Urban");
        valToWrite.set(NumberFormat.getInstance().format(percUrban));
//        context.write(keyToWrite, valToWrite);
        //5th
        keyToWrite.set("5. Median value of occupied houses");
        long finalOwnOcc = 0;
        for (Long ownOccListFinal1 : ownOccListFinal) {
            finalOwnOcc += ownOccListFinal1;
        }
        Integer ownOccHousesIndex = 0;
        long medianForOwnOccHouses = findMedianValue(finalOwnOcc);
        for (Long ownOccListFinal1 : ownOccListFinal) {
            medianForOwnOccHouses -= ownOccListFinal1;
            if (medianForOwnOccHouses < 0) {
                ownOccHousesIndex = ownOccListFinal.indexOf(ownOccListFinal1);
                break;
            }
        }
        valToWrite.set(ownOccList.get(ownOccHousesIndex));
//        context.write(keyToWrite, valToWrite);
        //6th
        keyToWrite.set("6. Median value of rent paid");
        long finalRentPaid = 0;
        for (Long rent : rentListFinal) {
            finalRentPaid += rent;
        }
        Integer rentPaidIndex = 0;
        long medianForRentPaid = findMedianValue(finalRentPaid);
        for (Long rent : rentListFinal) {
            medianForRentPaid -= rent;
            if (medianForRentPaid < 0) {
                rentPaidIndex = rentListFinal.indexOf(rent);
                break;
            }
        }
        valToWrite.set(rentedList.get(rentPaidIndex));
        //7th
        long totRoom = 0;
        long rooms = 0;
        int index = 1;
        for (Long roomFinal : roomListFinal) {
            rooms += (roomFinal * index);
            totRoom += roomFinal;
            ++index;
        }
        double avgRooms = (double) rooms / totRoom;
        //8th
        double percOfElder = agegt85 * 100d / totPplAge;
//        context.write(keyToWrite, valToWrite);
        context.write(key, new Text(String.format("%.2f", percRented) + Constants.delim + String.format("%.2f", percOwned) + Constants.delim + String.format("%.2f", percmaleNeverMarried) + Constants.delim + String.format("%.2f", percfemaleNeverMarried) + Constants.delim + String.format("%.2f", percMaleLt18) + Constants.delim + String.format("%.2f", percMalebt19and29) + Constants.delim + String.format("%.2f", percMalebt30and39) + Constants.delim + String.format("%.2f", percFemaleLt18) + Constants.delim + String.format("%.2f", percFemalebt19and29) + Constants.delim + String.format("%.2f", percFemalebt30and39) + Constants.delim + String.format("%.2f", percRural) + Constants.delim + String.format("%.2f", percUrban) + Constants.delim + ownOccList.get(ownOccHousesIndex) + Constants.delim + rentedList.get(rentPaidIndex) + Constants.delim + avgRooms + Constants.delim + percOfElder));
    }

    public long findMedianValue(long allValues) {
        if (allValues % 2 == 0) {
            return ((allValues / 2) + 1);
        } else {
            return (allValues + 1) / 2;
        }
    }
}
