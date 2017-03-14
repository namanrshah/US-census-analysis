/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cs455.hadoop.uscensus;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author namanrs
 */
public class MapperOPValue implements Writable {

    Text state;
    IntWritable segmentNo;
    //1st//
    LongWritable totRented;
    LongWritable totOwned;
//    LongWritable totHouses;
    //2nd//
    LongWritable maleNeveMarried;
//    LongWritable totMale;
    LongWritable femaleNeverMarried;
//    LongWritable totFemale;
    LongWritable population;
    //3rd//
    LongWritable maleLt18;
    LongWritable maleBt19and29;
    LongWritable maleBt30and39;
    LongWritable totMaleAge;

    LongWritable femaleLt18;
    LongWritable femaleBt19and29;
    LongWritable femaleBt30and39;
    LongWritable totFemaleAge;
    //4th//
    LongWritable totUrbanHH;
    LongWritable totRuralHH;
    LongWritable totHH;
    //5th//
    Text ownOccList;
    //6th//
    Text rentList;
    //7th//
    Text roomList;
    //8th
    LongWritable agegt85;
    LongWritable totPplAge;

    public MapperOPValue() {
        this.state = new Text();
        this.segmentNo = new IntWritable();
        this.totRented = new LongWritable();
        this.totOwned = new LongWritable();
        this.maleNeveMarried = new LongWritable();
        this.femaleNeverMarried = new LongWritable();
        this.population = new LongWritable();
        this.maleLt18 = new LongWritable();
        this.maleBt19and29 = new LongWritable();
        this.maleBt30and39 = new LongWritable();
        this.totMaleAge = new LongWritable();
        this.femaleLt18 = new LongWritable();
        this.femaleBt19and29 = new LongWritable();
        this.femaleBt30and39 = new LongWritable();
        this.totFemaleAge = new LongWritable();
        this.totUrbanHH = new LongWritable();
        this.totRuralHH = new LongWritable();
        this.totHH = new LongWritable();
        this.ownOccList = new Text();
        this.rentList = new Text();
        this.roomList = new Text();
        this.agegt85 = new LongWritable();
        this.totPplAge = new LongWritable();
    }

    public LongWritable getPopulation() {
        return population;
    }

    public void setPopulation(LongWritable population) {
        this.population = population;
    }

    public Text getState() {
        return state;
    }

    public void setState(Text state) {
        this.state = state;
    }

    public IntWritable getSegmentNo() {
        return segmentNo;
    }

    public void setSegmentNo(IntWritable segmentNo) {
        this.segmentNo = segmentNo;
    }

    public LongWritable getTotRented() {
        return totRented;
    }

    public void setTotRented(LongWritable totRented) {
        this.totRented = totRented;
    }

    public LongWritable getTotOwned() {
        return totOwned;
    }

    public void setTotOwned(LongWritable totOwned) {
        this.totOwned = totOwned;
    }

    public LongWritable getMaleNeveMarried() {
        return maleNeveMarried;
    }

    public void setMaleNeveMarried(LongWritable maleNeveMarried) {
        this.maleNeveMarried = maleNeveMarried;
    }

    public LongWritable getFemaleNeverMarried() {
        return femaleNeverMarried;
    }

    public void setFemaleNeverMarried(LongWritable femaleNeverMarried) {
        this.femaleNeverMarried = femaleNeverMarried;
    }

    public LongWritable getMaleLt18() {
        return maleLt18;
    }

    public void setMaleLt18(LongWritable maleLt18) {
        this.maleLt18 = maleLt18;
    }

    public LongWritable getMaleBt19and29() {
        return maleBt19and29;
    }

    public void setMaleBt19and29(LongWritable maleBt19and29) {
        this.maleBt19and29 = maleBt19and29;
    }

    public LongWritable getMaleBt30and39() {
        return maleBt30and39;
    }

    public void setMaleBt30and39(LongWritable maleBt30and39) {
        this.maleBt30and39 = maleBt30and39;
    }

    public LongWritable getTotMaleAge() {
        return totMaleAge;
    }

    public LongWritable getFemaleLt18() {
        return femaleLt18;
    }

    public void setFemaleLt18(LongWritable femaleLt18) {
        this.femaleLt18 = femaleLt18;
    }

    public LongWritable getFemaleBt19and29() {
        return femaleBt19and29;
    }

    public void setFemaleBt19and29(LongWritable femaleBt19and29) {
        this.femaleBt19and29 = femaleBt19and29;
    }

    public LongWritable getFemaleBt30and39() {
        return femaleBt30and39;
    }

    public void setFemaleBt30and39(LongWritable femaleBt30and39) {
        this.femaleBt30and39 = femaleBt30and39;
    }

    public LongWritable getTotFemaleAge() {
        return totFemaleAge;
    }

    public void setTotFemaleAge(LongWritable totFemaleAge) {
        this.totFemaleAge = totFemaleAge;
    }

    public void setTotMaleAge(LongWritable totMaleAge) {
        this.totMaleAge = totMaleAge;
    }

    public LongWritable getTotUrbanHH() {
        return totUrbanHH;
    }

    public void setTotUrbanHH(LongWritable totUrbanHH) {
        this.totUrbanHH = totUrbanHH;
    }

    public LongWritable getTotRuralHH() {
        return totRuralHH;
    }

    public void setTotRuralHH(LongWritable totRuralHH) {
        this.totRuralHH = totRuralHH;
    }

    public LongWritable getTotHH() {
        return totHH;
    }

    public void setTotHH(LongWritable totHH) {
        this.totHH = totHH;
    }

    public Text getOwnOccList() {
        return ownOccList;
    }

    public void setOwnOccList(Text ownOccList) {
        this.ownOccList = ownOccList;
    }

    public Text getRentList() {
        return rentList;
    }

    public void setRentList(Text rentList) {
        this.rentList = rentList;
    }

    public Text getRoomList() {
        return roomList;
    }

    public void setRoomList(Text roomList) {
        this.roomList = roomList;
    }

    public LongWritable getAgegt85() {
        return agegt85;
    }

    public void setAgegt85(LongWritable agegt85) {
        this.agegt85 = agegt85;
    }

    public LongWritable getTotPplAge() {
        return totPplAge;
    }

    public void setTotPplAge(LongWritable totPplAge) {
        this.totPplAge = totPplAge;
    }

    @Override
    public void write(DataOutput d) throws IOException {
        agegt85.write(d);
        femaleBt19and29.write(d);
        femaleBt30and39.write(d);
        femaleLt18.write(d);
        femaleNeverMarried.write(d);
        maleBt19and29.write(d);
        maleBt30and39.write(d);
        maleLt18.write(d);
        maleNeveMarried.write(d);
        ownOccList.write(d);
        population.write(d);
        rentList.write(d);
        roomList.write(d);
        segmentNo.write(d);
        state.write(d);
        totFemaleAge.write(d);
        totHH.write(d);
        totMaleAge.write(d);
        totOwned.write(d);
        totPplAge.write(d);
        totRented.write(d);
        totRuralHH.write(d);
        totUrbanHH.write(d);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        agegt85.readFields(di);
        femaleBt19and29.readFields(di);
        femaleBt30and39.readFields(di);
        femaleLt18.readFields(di);
        femaleNeverMarried.readFields(di);
        maleBt19and29.readFields(di);
        maleBt30and39.readFields(di);
        maleLt18.readFields(di);
        maleNeveMarried.readFields(di);
        ownOccList.readFields(di);
        population.readFields(di);
        rentList.readFields(di);
        roomList.readFields(di);
        segmentNo.readFields(di);
        state.readFields(di);
        totFemaleAge.readFields(di);
        totHH.readFields(di);
        totMaleAge.readFields(di);
        totOwned.readFields(di);
        totPplAge.readFields(di);
        totRented.readFields(di);
        totRuralHH.readFields(di);
        totUrbanHH.readFields(di);
    }
}
