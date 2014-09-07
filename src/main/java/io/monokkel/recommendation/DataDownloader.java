package io.monokkel.recommendation;

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

import static java.lang.String.format;

/**
 * Created by tarjei on 22/08/14.
 */
public class DataDownloader {

    private static final Logger log = LogManager.getLogger(DataDownloader.class);

    public static void main(final String[] args) {

        DataDownloader dataDownloader = new DataDownloader();

        final String consumerKey = "jtUBU2lkYGCKpWmoetljSWmC5";
        final String consumerSecret = "Zz5blHwOIzvCMprj0Ct705umT5BZ5R56JYazLOA4OJsXWSxhzC";
        final String accessToken = "96370324-PhI7uyoWMADfuYJq53vP249VNrV0eTu3xriPXFg1o";
        final String accessTokenSecret = "9m6cGXn6EZcCgQdlwDM2TZ9wJkIoMStzvX8MCGULVu2qS";
        final String dataFileName = "datafile.csv";

        dataDownloader.run(consumerKey, consumerSecret, accessToken, accessTokenSecret, dataFileName);



    }

    private void run(final String consumerKey, final String consumerSecret, final String accessToken, final String accessTokenSecret, final String pathname) {

        File dataFile = new File(pathname);
        final String headerLine = "tweetId,timelineOwner,isRetweet,isRetweeted,retweetCount,isFavorited,favoritedCount,languageCode,hasTags,hasMedia,hasMentions,hasUrls,createdAt,originalTweetId,hashTags\n";

        writeLineToFile(dataFile, headerLine, false);

        Twitter twitter = new TwitterTemplate(consumerKey, consumerSecret, accessToken, accessTokenSecret);
        final UserOperations userOperations = twitter.userOperations();
        final Long profileId = userOperations.getProfileId();
        final CursoredList<Long> friendIds = twitter.friendOperations().getFriendIds();

        downloadAndStoreAndHandleRateLimit(dataFile, twitter, profileId);

        friendIds.stream().forEach(friendId -> downloadAndStoreAndHandleRateLimit(dataFile, twitter, friendId));
    }

    private void writeLineToFile(final File dataFile, final String headerLine, final boolean appendToFIle) {
        try {
            FileUtils.writeStringToFile(dataFile, headerLine, appendToFIle);
        } catch (IOException e) {
            log.error("Failed to write line to CVS", e);
        }
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
                    tweet.hasTags(), tweet.hasMedia(), tweet.hasMentions(), tweet.hasUrls(), tweet.getCreatedAt().getTime(), retweetedId, commaSeparatedHashTag);
            writeLineToFile(dataFile, line, true);
        });
    }


    private void downloadAndStoreAndHandleRateLimit(File dataFile, Twitter twitter, Long profileId) {

        log.info("Retrieving profile id {}", profileId);

        while (true) { // Retry until break is called
            try {
                storeTweetsInDataFile(dataFile, twitter, profileId);
                break;
            } catch (RateLimitExceededException e) {
                log.info("Rate limit reached");
                sleepTheRateLimitAway(twitter);
                log.info("Retrying retrieving profile id {}", profileId);
            }
        }
    }

    private void sleepTheRateLimitAway(Twitter twitter) {
        final Map<ResourceFamily, List<RateLimitStatus>> rateLimitStatus = twitter.userOperations().getRateLimitStatus(ResourceFamily.STATUSES);
        rateLimitStatus.forEach((resourceFamily, rateLimitStatuses) -> rateLimitStatuses.stream().forEach(rateLimit ->
        {
            final int remainingHits = rateLimit.getRemainingHits();
            if (remainingHits == 0) {
                final Date resetTime = rateLimit.getResetTime();
                final Long resetUnixTime = resetTime.getTime() + 10000 - System.currentTimeMillis();
                try {
                    log.info("Waiting {} ms", remainingHits);
                    Thread.sleep(resetUnixTime);
                } catch (InterruptedException e) {
                }
            }
        }));

    }
}
