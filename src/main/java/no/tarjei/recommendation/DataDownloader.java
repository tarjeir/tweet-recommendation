package no.tarjei.recommendation;

import io.prediction.Client;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.social.RateLimitExceededException;
import org.springframework.social.twitter.api.*;
import org.springframework.social.twitter.api.impl.TwitterTemplate;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.lang.String.format;

/**
 * Created by tarjei on 22/08/14.
 */
public class DataDownloader {

    private static final Logger log = LogManager.getLogger(DataDownloader.class);

    public static void main(final String[] args) {

        DataDownloader dataDownloader = new DataDownloader();
        dataDownloader.run();

    }

    private void run() {

        String consumerKey = "jtUBU2lkYGCKpWmoetljSWmC5"; // The application's consumer key
        String consumerSecret = "Zz5blHwOIzvCMprj0Ct705umT5BZ5R56JYazLOA4OJsXWSxhzC"; // The application's consumer secret
        String accessToken = "96370324-PhI7uyoWMADfuYJq53vP249VNrV0eTu3xriPXFg1o"; // The access token granted after OAuth authorization
        String accessTokenSecret = "9m6cGXn6EZcCgQdlwDM2TZ9wJkIoMStzvX8MCGULVu2qS"; // The access token secret granted after OAuth authorization
        File dataFile = new File("datafile.csv");
        try {
            FileUtils.writeStringToFile(dataFile, "tweetId,timelineOwner,isRetweet,isRetweeted,retweetCount,isFavorited,favoritedCount,languageCode,hasTags,hasMedia,hasMentions,hasUrls,createdAt,hashTags\n");
        } catch (IOException e) {
            e.printStackTrace();
        }

        Twitter twitter = new TwitterTemplate(consumerKey, consumerSecret, accessToken, accessTokenSecret);
        final UserOperations userOperations = twitter.userOperations();
        final Long profileId = userOperations.getProfileId();


        final CursoredList<Long> friendIds = twitter.friendOperations().getFriendIds();

        downloadAndStoreAndHandleRateLimit(dataFile, twitter, profileId);

        friendIds.stream().forEach(friendId -> {
            downloadAndStoreAndHandleRateLimit(dataFile, twitter, friendId);
        });
    }

    private void storeTweetsInDataFile(File dataFile, Twitter twitter, Long profileId) {
        final TimelineOperations timelineOperations = twitter.timelineOperations();

        final List<Tweet> tweets = timelineOperations.getUserTimeline(profileId, 200);

        tweets.stream().forEach(tweet -> {

            final String commaSeparatedHashTag = tweet.getEntities().getHashTags().stream()
                    .map(HashTagEntity::getText)
                    .reduce("", (prev, next) -> format("%s,%s", prev, next));

            final Serializable retweetedId = tweet.isRetweet() ? tweet.getRetweetedStatus().getId() : "0";

            String line = format("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s%s\n", tweet.getId(), tweet.getUser().getId(), tweet.isRetweet(), tweet.isRetweeted(),
                    tweet.getRetweetCount(), tweet.isFavorited(), tweet.getFavoriteCount(), tweet.getLanguageCode(),
                    tweet.hasTags(), tweet.hasMedia(), tweet.hasMentions(), tweet.hasUrls(), tweet.getCreatedAt().getTime(),retweetedId,commaSeparatedHashTag);
            try {
                FileUtils.writeStringToFile(dataFile, line, true);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }


    private void downloadAndStoreAndHandleRateLimit(File dataFile, Twitter twitter, Long profileId) {

        log.info("Retrieving profile id %s", profileId);

        while (true) { // Retry until break is called
            try {
                storeTweetsInDataFile(dataFile, twitter, profileId);
                break;
            } catch (RateLimitExceededException e) {
                log.info("Rate limit reached");
                sleepTheRateLimitAway(twitter);
                log.info("Retrying retrieving profile id %s", profileId);
            }
        }
    }

    private void sleepTheRateLimitAway(Twitter twitter) {
        final Map<ResourceFamily, List<RateLimitStatus>> rateLimitStatus = twitter.userOperations().getRateLimitStatus(ResourceFamily.STATUSES);

        for (Map.Entry<ResourceFamily, List<RateLimitStatus>> rateLimitEntry : rateLimitStatus.entrySet()) {
            for (RateLimitStatus rateLimit : rateLimitEntry.getValue()) {
                final int remainingHits = rateLimit.getRemainingHits();
                if (remainingHits == 0) {
                    final Date resetTime = rateLimit.getResetTime();
                    final Long resetUnixTime = resetTime.getTime() + 10000 - System.currentTimeMillis();
                    try {
                        log.info("Waiting %s ms", remainingHits);
                        Thread.sleep(resetUnixTime);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }

        }
    }
}
