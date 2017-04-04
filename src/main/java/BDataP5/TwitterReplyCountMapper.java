package BDataP5;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import twitter4j.*;

import java.io.IOException;
/**
 * Created by lsolorzano on 3/25/2017.
 */
public class TwitterReplyCountMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    @Override
    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {

        String rawTweet = value.toString();

        try {
            Status status = TwitterObjectFactory.createStatus(rawTweet);

            context.write(new Text(Long.toString(status.getInReplyToStatusId())), new Text(String.valueOf(status.getId())));

        }
        catch(TwitterException e){
        }

    }
}

