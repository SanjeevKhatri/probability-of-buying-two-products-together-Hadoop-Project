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

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by cloudera on 10/18/16.
 */
public class CrystalBallStripe {

    /******************* MAPPER *********************/
    public static class CrystalBallStripeMapper extends Mapper<LongWritable, Text, Text, StringDoubleMapWritable>
    {
        MapWritable bucket;
        @Override
        protected void setup(Mapper<LongWritable, Text, Text, StringDoubleMapWritable>.Context context) throws IOException, InterruptedException
        {
            bucket = new MapWritable();
            super.setup(context);
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            String[] productIds = value.toString().split("\\s+");

            StringDoubleMapWritable currentStripe;
            for(int i = 1; i < productIds.length - 1; i++)
            {
                currentStripe = new StringDoubleMapWritable();
                String currentProductId = productIds[i];
                boolean rangeCompleted = false;

                for(int j = i + 1; ((j < productIds.length) && !rangeCompleted); j++) {
                    String currentNeighborId = productIds[j];
                    if(currentProductId.equals(currentNeighborId))
                    {
                        rangeCompleted = true;
                    }
                    else
                    {
                        if( ! currentStripe.hasKey(currentNeighborId))
                        {
                            currentStripe.setValue(currentNeighborId, 0.0);
                        }
                        currentStripe.sumToValue(currentNeighborId, 1.0);
                    }
                }


                Text textProductId = new Text(currentProductId);
                if( ! bucket.containsKey(textProductId))
                {
                    bucket.put(textProductId, currentStripe);
                }
                else
                {
                    bucket.put(textProductId, ((StringDoubleMapWritable)bucket.get(textProductId)).sumMap(currentStripe));
                }
            }
        }


        @Override
        protected void cleanup(Mapper<LongWritable, Text, Text, StringDoubleMapWritable>.Context context) throws IOException, InterruptedException
        {
            Iterator<Writable> bucketIterator = bucket.keySet().iterator();

            while(bucketIterator.hasNext()) {
                Text bucketKey = (Text) bucketIterator.next();
                context.write(bucketKey, (StringDoubleMapWritable) bucket.get(bucketKey));
                System.out.println(bucketKey + ((StringDoubleMapWritable)bucket.get(bucketKey)).toString());
            }
            super.cleanup(context);
        }
    }

    public static class CrystalBallStripePartitioner extends Partitioner<Text, StringDoubleMapWritable> {
        @Override
        public int getPartition(Text key, StringDoubleMapWritable value, int i) {
            if(Integer.parseInt(key.toString()) < 30) return 0;
            if(Integer.parseInt(key.toString()) < 60) return 1;
            return 2;
        }
    }


    /************************ REDUCER ************************/
    public static class CrystalBallStripeReducer extends Reducer<Text, StringDoubleMapWritable, Text, Text>
    {
        @Override
        public void reduce(Text productId, Iterable<StringDoubleMapWritable> stripes, Context context) throws IOException, InterruptedException
        {
            StringDoubleMapWritable finalStripe = new StringDoubleMapWritable();
            double total = 0;
            for(StringDoubleMapWritable newStripe : stripes)
            {
                total += newStripe.sumValues();
                finalStripe.sumMap(newStripe);
            }
            finalStripe.averageElements(total);

            context.write(new Text(productId.toString()), new Text(finalStripe.toString()));
        }
    }

    public static class StringDoubleMapWritable extends MapWritable{

        public StringDoubleMapWritable()
        {
            super();
        }

        public void setValue(String key, Double value)
        {
            put(new Text(key), new DoubleWritable(value));
        }

        public double getValue(String key)
        {
            return ((DoubleWritable)get(new Text(key))).get();
        }

        public boolean hasKey(String key)
        {
            return containsKey(new Text(key));
        }

        public void sumToValue(String key, double toAdd)
        {
            Text textKey = new Text(key);
            double previousValue = ((DoubleWritable)get(textKey)).get();
            double nextValue = previousValue + toAdd;
            super.put(textKey, new DoubleWritable(nextValue));
        }

        public double sumValues()
        {
            double sum = 0;
            Iterator<Writable> iterator = keySet().iterator();
            while(iterator.hasNext()) {
                String key = ((Text) iterator.next()).toString();
                sum += getValue(key);
            }
            return sum;
        }

        public StringDoubleMapWritable sumMap(StringDoubleMapWritable anotherMap)
        {
            Iterator<Writable> anotherMapIterator = anotherMap.keySet().iterator();

            while(anotherMapIterator.hasNext())
            {
                String anotherMapKey = ((Text) anotherMapIterator.next()).toString();
                if (hasKey(anotherMapKey)) {
                    sumToValue(anotherMapKey, anotherMap.getValue(anotherMapKey));
                } else {
                    setValue(anotherMapKey, anotherMap.getValue(anotherMapKey));
                }
            }
            return this;
        }

        public StringDoubleMapWritable averageElements(double total)
        {
            Iterator<Writable> iterator = keySet().iterator();

            while(iterator.hasNext())
            {
                String mapKey = ((Text) iterator.next()).toString();
                double average = getValue(mapKey) / total;
                setValue(mapKey, average);
            }
            return this;
        }



        @Override
        public String toString()
        {
            Iterator<Writable> iterator = keySet().iterator();
            String result = "{";
            while(iterator.hasNext())
            {
                String key = ((Text)iterator.next()).toString();
                double value = getValue(key);
                result += "(" + key + ", " + value + "), ";
            }
            result += "}";
            return result;
        }

    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Crystal Ball");

        job.setJarByClass(CrystalBallStripe.class);
        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(new Path("CrystalBallStripe"), true);

        FileInputFormat.addInputPath(job, new Path("input"));
        FileOutputFormat.setOutputPath(job, new Path("CrystalBallStripe"));

        job.setMapperClass(CrystalBallStripeMapper.class);
        job.setReducerClass(CrystalBallStripeReducer.class);

        job.setNumReduceTasks(3);
        job.setPartitionerClass(CrystalBallStripePartitioner.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(StringDoubleMapWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}

