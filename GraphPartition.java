import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Vertex implements Writable {
    public long id;                   // the vertex ID
    public Vector<Long> adjacent;     // the vertex neighbors
    public long centroid;             // the id of the centroid in which this vertex belongs to
    public short depth;
    public long lst_sz; 

    public Vertex()
    {}

    public Vertex(long id, Vector<Long> adjacent, long centroid, short depth)
    {
        this.id = id ; 
        this.adjacent = adjacent; 
        this.centroid = centroid;
        this.depth = depth; 
        lst_sz = adjacent.size();
            
    } 

    //@Override
    public void readFields(DataInput in) throws IOException
    {
        id = in.readLong();
        centroid = in.readLong();
        adjacent = new Vector<Long>(); 
        depth = in.readShort();
        lst_sz = in.readLong();
        for(int i = 0; i<lst_sz; i++)
        {
            adjacent.add(in.readLong());
        }
    }
    //@Override
    public void write(DataOutput out) throws IOException
    {
        out.writeLong(id);
        out.writeLong(centroid);
        out.writeShort(depth);
        out.writeLong(lst_sz);
        for(int i = 0 ; i<lst_sz; i++)
        {
            out.writeLong(adjacent.get(i));
        }       
}
}

public class GraphPartition {
    static Vector<Long> centroids = new Vector<Long>();
    final static short max_depth = 8;
    static short BFS_depth = 0;

public static class Mapper1 extends Mapper<Object, Text, LongWritable, Vertex>
{ 
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
        String f = value.toString();
        String[] line_split = f.split(",");
        long id = Long.parseLong(line_split[0]); 
        long centroid; 
        Vector<Long> adjacent = new Vector<Long>(); 
        short s = 0;
        for(int i = 1; i <line_split.length; i++)
        {
            adjacent.add(Long.parseLong(line_split[i]));
        }
        if(centroids.size() <10)
        {
            centroids.add(id);
            centroid = id;
        }
        else
        {
            centroid = -1; 
        }
        LongWritable l = new LongWritable(id);
        System.out.println(id + " " + adjacent);
        context.write(l, new Vertex(id, adjacent, centroid, s));
}
}

public static class Mapper2 extends Mapper<LongWritable,Vertex, LongWritable, Vertex>{
        
        public void map(LongWritable key, Vertex v, Context context) throws IOException,InterruptedException{
        context.write(new LongWritable(v.id), v);
       
            if (v.centroid > 0)
            {
                for (Long n : v.adjacent)
                {
                    context.write(new LongWritable(n), new Vertex(n, new Vector<Long>(), v.centroid, BFS_depth));
                }
            }
        }
    }

public static class Reducer2 extends Reducer<LongWritable, Vertex, LongWritable, Vertex>{

    @Override
    public void reduce(LongWritable key, Iterable<Vertex> v, Context context) throws IOException,InterruptedException
    {

        short min_depth = 1000;
        Vertex m = new Vertex(key.get(),new Vector<Long>(),(long)(-1),(short)(0));
        for(Vertex x : v)
        {
            if(!(x.adjacent.isEmpty()))
            {
                m.adjacent = x.adjacent;
            }
            if(x.centroid > 0 && x.depth < min_depth)
            {
                min_depth = x.depth;
                m.centroid = x.centroid;
            }


        }
        m.depth = min_depth;
        context.write(key, m);
    }
}

public static class Mapper3 extends Mapper<LongWritable,Vertex, LongWritable, IntWritable>{
        @Override 
        public void map(LongWritable centroid, Vertex v, Context context) throws IOException,InterruptedException
        {
            System.out.println(centroid + " " + v.centroid);
            context.write(new LongWritable(v.centroid) ,new IntWritable(1));

        }
}

public static class Reducer3 extends Reducer<LongWritable, IntWritable, LongWritable, LongWritable>{

    @Override
    public void reduce(LongWritable key, Iterable<IntWritable> v, Context context) throws IOException,InterruptedException
    {
        long m = 0L;
        for (IntWritable i : v)
        {
            m = m + Long.valueOf(i.get()) ;
        }
        context.write(key, new LongWritable(m));


}
}


    public static void main ( String[] args ) throws Exception {
        Job job = Job.getInstance();
        job.setJobName("Job1");
        /* ... First Map-Reduce job to read the graph */
        job.setJarByClass(GraphPartition.class);
        
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Vertex.class);
        
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Vertex.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,Mapper1.class);
        FileOutputFormat.setOutputPath(job,new Path(args[1]+"/i0"));

        job.waitForCompletion(true);

        for (short i = 0; i < max_depth; i++ ) {
            BFS_depth++;
            job = Job.getInstance();
            /* ... Second Map-Reduce job to do BFS */

            job.setJobName("Job2");
        /* ... First Map-Reduce job to read the graph */
            job.setJarByClass(GraphPartition.class);
        
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(Vertex.class);
        
            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(Vertex.class);
        
            job.setMapperClass(Mapper2.class);
            job.setReducerClass(Reducer2.class);

            job.setOutputFormatClass(SequenceFileOutputFormat.class);
        
            MultipleInputs.addInputPath(job,new Path(args[1]+"/i"+i),SequenceFileInputFormat.class,Mapper2.class);
            FileOutputFormat.setOutputPath(job,new Path(args[1]+"/i"+(i+1)));
            job.waitForCompletion(true);
        }

        job = Job.getInstance();
        job.setJobName("job3");
        job.setJarByClass(GraphPartition.class);
        job.setReducerClass(Reducer3.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);
    
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        MultipleInputs.addInputPath(job,new Path(args[1]+"/i8"),SequenceFileInputFormat.class,Mapper3.class);
        FileOutputFormat.setOutputPath(job,new Path(args[2]));
        job.waitForCompletion(true);
    }
}
