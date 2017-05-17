import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.oncrpc.security.SysSecurityHandler;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by cloudera on 10/18/16.
 */
public class CrystalBallHybrid {

    /******************* MAPPER *********************/
    public static class CrystalBallHybridMapper extends Mapper<LongWritable, Text, Pair, IntWritable>
    {
        Map<Pair, Integer> bucket;

        @Override
        protected void setup(Mapper<LongWritable, Text, Pair, IntWritable>.Context context) throws IOException, InterruptedException
        {
            bucket = new HashMap<>();
            super.setup(context);
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            String[] productIds = value.toString().split("\\s+");

            for(int i = 1; i < productIds.length - 1; i++)
            {
                String currentProductId = productIds[i];
                boolean rangeCompleted = false;

                for(int j = i + 1; ((j < productIds.length) && !rangeCompleted); j++)
                {
                    String currentNeighborId = productIds[j];
                    if(currentProductId.equals(currentNeighborId))
                    {
                        rangeCompleted = true;
                    }
                    else
                    {
                        Pair newPair = new Pair(currentProductId, currentNeighborId);
                        if(!bucket.containsKey(newPair))
                        {
                            bucket.put(newPair, 0);
                        }
                        bucket.put(newPair,  bucket.get(newPair) + 1);
                    }
                }
            }
        }

        @Override
        protected void cleanup(Mapper<LongWritable, Text, Pair, IntWritable>.Context context) throws IOException, InterruptedException
        {
            Iterator<Pair> bucketIterator = bucket.keySet().iterator();

            while(bucketIterator.hasNext())
            {
                Pair key = bucketIterator.next();
                context.write(key, new IntWritable(bucket.get(key)));
            }
            super.cleanup(context);
        }
    }


    public static class CrystalBallHybridPartitioner extends Partitioner<Pair, IntWritable> {
        @Override
        public int getPartition(Pair pair, IntWritable intWritable, int i) {
            if(Integer.parseInt(pair.getFirst()) < 50) return 0;
            return 1;
        }
    }


            /************************ REDUCER ************************/
    public static class CrystalBallHybridReducer extends Reducer<Pair, IntWritable, Text, Text>
    {
        CustomMapWritable stripe;
        int totalNeighbors;
        Text currentProductId;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            stripe = new CustomMapWritable();
            totalNeighbors = 0;
            currentProductId = null;
        }

        @Override
        public void reduce(Pair productNeighborPair, Iterable<IntWritable> frequencies, Context context) throws IOException, InterruptedException
        {
            Text currentProductNeighborId = new Text(productNeighborPair.getSecond());

            if(currentProductId == null)
            {
                currentProductId = new Text(productNeighborPair.getFirst());
            }
            else if( ! currentProductId.toString().equals(productNeighborPair.getFirst()))
            {
                Iterator<Writable> stripeIterator = stripe.keySet().iterator();

                while(stripeIterator.hasNext())
                {
                    Writable mapKey = stripeIterator.next();
                    double average = ((double)((IntWritable)stripe.get(mapKey)).get()) / totalNeighbors;
                    stripe.put(mapKey, new DoubleWritable(average));
                }
                context.write(currentProductId, new Text(stripe.toString()));
                stripe = new CustomMapWritable();
                totalNeighbors = 0;
                currentProductId = new Text(productNeighborPair.getFirst());
            }
            int totalFrequency = 0;
            for(IntWritable frequency : frequencies) {
                totalFrequency += frequency.get();
            }
            totalNeighbors += totalFrequency;

            if( stripe.containsKey(currentProductNeighborId))
            {
                int sum = ((IntWritable)stripe.get(currentProductNeighborId)).get() + totalFrequency;
                stripe.put(currentProductNeighborId, new IntWritable(sum));
            }
            else
            {
                stripe.put(currentProductNeighborId, new IntWritable(totalFrequency));
            }

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Iterator<Writable> stripeIterator = stripe.keySet().iterator();

            while(stripeIterator.hasNext())
            {
                Text mapKey = (Text) stripeIterator.next();
                double average = ((double)((IntWritable)stripe.get(mapKey)).get()) / totalNeighbors;
                stripe.put(mapKey, new DoubleWritable(average));
            }
            context.write(currentProductId, new Text(stripe.toString()));
            super.cleanup(context);
        }

    }

    public static class CustomMapWritable extends MapWritable{
        public CustomMapWritable()
        {
            super();
        }
        @Override
        public String toString()
        {
            Iterator<Writable> iterator = keySet().iterator();
            String result = "{";
            while(iterator.hasNext())
            {
                Text key = (Text)iterator.next();
                Writable value = get(key);
                result += "(" + key + ", " + value + "), ";
            }
            result += "}";
            return result;
        }
    }




    public static class Pair implements WritableComparable<Pair>, Writable
    {
        private String first;
        private String second;
        public String neighborWildCard = "*";

        public Pair(){
            super();
        }

        public Pair(String first, String second){
            super();
            this.first = first;
            this.second = second;
        }

        public Pair(String first)
        {
            super();
            this.first = first;
            this.second = neighborWildCard;
        }

        public String getFirst() {
            return first;
        }

        public void setFirst(String first) {
            this.first = first;
        }

        public String getSecond() {
            return second;
        }

        public void setSecond(String second) {
            this.second = second;
        }

        @Override
        public void readFields(DataInput input) throws IOException {
            first = input.readUTF();
            second = input.readUTF();
        }

        @Override
        public void write(DataOutput output) throws IOException {
            output.writeUTF(first);
            output.writeUTF(second);
        }

        @Override
        public int hashCode()
        {
            int prime = 31;
            int result = 0;
            result = prime * result + first.hashCode();
            return prime * result + second.hashCode();
        }

        @Override
        public boolean equals(Object obj)
        {
            if(this == obj) return true;
            if(!(obj instanceof Pair)) return false;
            Pair p = (Pair) obj;
            if(! p.getFirst().equals(this.first)) return false;
            return (p.getSecond().equals(this.second));
        }

        @Override
        public String toString() {
            return new String("[" + first + ", " + second + "]");
        }

        @Override
        public int compareTo(Pair o) {
            Pair p = (Pair) o;
            int toReturn = 0;
            if(! first.equals(p.getFirst())) return first.compareTo(p.getFirst());
            if(isWildCard()) return -1;
            if(o.isWildCard()) return 1;
            if(p.isWildCard()) return -1;
            return second.compareTo(p.getSecond());

        }

        public boolean isWildCard()
        {
            return second.equals(neighborWildCard);
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Crystal Ball");

        job.setJarByClass(CrystalBallHybrid.class);
        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(new Path("CrystalBallHybrid"), true);
//
        FileInputFormat.addInputPath(job, new Path("input"));
        FileOutputFormat.setOutputPath(job, new Path("CrystalBallHybrid"));

        job.setMapperClass(CrystalBallHybridMapper.class);
        job.setReducerClass(CrystalBallHybridReducer.class);

        job.setNumReduceTasks(2);
        job.setPartitionerClass(CrystalBallHybridPartitioner.class);

        job.setMapOutputKeyClass(Pair.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}

