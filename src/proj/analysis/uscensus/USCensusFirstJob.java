/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package proj.analysis.uscensus;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author namanrs
 */
public class USCensusFirstJob {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            // Give the MapRed job a name. You'll see this name in the Yarn
            // webapp.
            Job job1 = Job.getInstance(conf, "US-Census-1");
            // Current class.
            job1.setJarByClass(USCensusFirstJob.class);
            // Mapper
            job1.setMapperClass(USCensusFirstMapper.class);
            // Combiner. We use the reducer as the combiner in this case.
            job1.setCombinerClass(USCensusFirstCombiner.class);
            // Reducer
            job1.setReducerClass(USCensusFirstReducer.class);
            job1.setPartitionerClass(USCensusFirstPartitioner.class);
            job1.setNumReduceTasks(USCensusFirstPartitioner.statesName.size());
            // Outputs from the Mapper.
            job1.setMapOutputKeyClass(Text.class);
            job1.setMapOutputValueClass(MapperOPValue.class);
            // Outputs from Reducer. It is sufficient to set only the following
            // two properties
            // if the Mapper and Reducer has same key and value types. It is set
            // separately for
            // elaboration.
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(Text.class);
            // path to input in HDFS
            FileInputFormat.addInputPath(job1, new Path(args[0]));
            FileSystem fileSystem = FileSystem.get(conf);

            String outputPath = args[1] + "_temp";

            if (fileSystem.exists(new Path(outputPath))) {
                fileSystem.delete(new Path(outputPath), true);
            }
            // path to output in HDFS
            FileOutputFormat.setOutputPath(job1, new Path(outputPath));
            // Block until the job is completed.
            boolean isCompleted = job1.waitForCompletion(true);
            //Second job
//            Configuration conf2 = new Configuration();
            if (isCompleted) {
                Job job2 = Job.getInstance(conf, "US-Census-2");
                job2.setJarByClass(USCensusFirstJob.class);
                // Mapper
                job2.setMapperClass(USCensusSecondMapper.class);
                // Reducer
                job2.setReducerClass(USCensusSecondReducer.class);
                job2.setNumReduceTasks(1);
                // Outputs from the Mapper.
                job2.setMapOutputKeyClass(Text.class);
                job2.setMapOutputValueClass(Text.class);
                // Outputs from Reducer. It is sufficient to set only the following
                // two properties
                // if the Mapper and Reducer has same key and value types. It is set
                // separately for
                // elaboration.
                job2.setOutputKeyClass(Text.class);
                job2.setOutputValueClass(Text.class);
                // path to input in HDFS
                FileInputFormat.addInputPath(job2, new Path(outputPath));
//            FileSystem fileSystem = FileSystem.get(conf1);

                String outputPath2 = args[1];

                if (fileSystem.exists(new Path(outputPath2))) {
                    fileSystem.delete(new Path(outputPath2), true);
                }
                // path to output in HDFS
                FileOutputFormat.setOutputPath(job2, new Path(outputPath2));
                System.exit(job2.waitForCompletion(true) ? 0 : 1);
            }
        } catch (IOException e) {
            System.err.println(e.getMessage());
        } catch (InterruptedException e) {
            System.err.println(e.getMessage());
        } catch (ClassNotFoundException e) {
            System.err.println(e.getMessage());
        }

    }

}
