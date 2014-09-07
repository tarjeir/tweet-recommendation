package io.monokkel.recommendation;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.List;
import java.util.Random;

/**
 * Created by tarjei on 17/08/14.
 */
public class RandomTest {

    @Test
    public void random_test(){
        Random random = new Random();
        String[] array = {"","","","","","","","","","","","","","","","","","","","",""};
        List<String> ratedTweets = Lists.newArrayList(array);

        for(int i = 0;i<1000;i++) {
            final Long randomTweetIndex = Math.round(random.nextGaussian() * (ratedTweets.size() * 0.10) + (ratedTweets.size() / 2) );

            System.out.println(randomTweetIndex);
        }

    }


}
