package com.yangguojun;


import javax.security.auth.login.Configuration;

public class WordCount {
    private static final Logger LOG = LoggerFactory.getLogger(WordCount.class);

    private static Configuration conf;

    static {
        String[] hosts = SystemConfig.getPropertyArray("game.x.hdfs.host", ",");
        conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://cluster1");
        conf.set("dfs.nameservices", "cluster1");
        conf.set("dfs.ha.namenodes.cluster1", "nna,nns");
        conf.set("dfs.namenode.rpc-address.cluster1.nna", hosts[0]);
        conf.set("dfs.namenode.rpc-address.cluster1.nns", hosts[1]);
        conf.set("dfs.client.failover.proxy.provider.cluster1", "org.apache.hadoop.hdfs.server.namenode.ha.ConfigureFailoverProxyProvider");
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    }

    public static  class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) {
            while (itr.hasMoreTokens()){
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWriteable, Text, IntWritable>{
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context){
            int sum = 0;
            for(Inwritable val : values){
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }


    public static void main(String[] args) {
        if(args.length < 1){
            LOG.info("args length is 0");
            run("hello.txt");
        }else{
            run(args[0]);
        }
    }

    private static void run(String name ){
        Job job = Job.getInstance(conf);
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        String tmpLocation = SystemConfig.getProperty("game.x.hdfs.input.path");
        String inPath = String.format(tmpLocation, name);
        String tmpLocalOut = SystemConfig.getProperty("game.x.hdfs.output.path");
        String outPath = String.format(tmpLocalOut, name);

        FileInputFormat.addInputPath(job, new Path(inPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));

        int status = job.waitForCompletion(true) ? 0 : 1;
        System.exit(status);
    }

}
