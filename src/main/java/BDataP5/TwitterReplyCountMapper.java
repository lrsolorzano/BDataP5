package BDataP5;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import twitter4j.*;

import java.io.IOException;
/**
 * Created by lsolorzano on 3/25/2017.
 */
public class TwitterReplyCountMapper extends Mapper<LongWritable, Text, LongWritable, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {

        String rawTweet = value.toString();

        try {
            Status status = TwitterObjectFactory.createStatus(rawTweet);
            long Reply_ID = status.getInReplyToUserId();

            //long originaluserid = originalTweet.getUser().getId();

            context.write(new Text(Long.toString(Reply_ID)), new IntWritable(1));

        }
        catch(TwitterException e){
        }
    }
}

