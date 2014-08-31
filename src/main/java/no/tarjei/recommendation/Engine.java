package no.tarjei.recommendation;

import com.google.common.collect.Lists;
import io.prediction.*;
import org.apache.commons.codec.binary.*;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.springframework.social.twitter.api.TimelineOperations;
import org.springframework.social.twitter.api.Tweet;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.Arrays.stream;
import static org.apache.commons.codec.binary.Base64.encodeBase64;

/**
 * Created by tarjei on 15/08/14.
 */
public class Engine {

    private static final Logger log = LogManager.getLogger(Engine.class);

    public static final String tokenizeRegExp = ",|/|\\.|\\s|\\t";

    public static final String FIRST_LINE = "tweetId,timelineOwner,isRetweet,isRetweeted,retweetCount,isFavorited,favoritedCount,languageCode,hasTags,hasMedia,hasMentions,hasUrls";

    public static final Double REETWEET_WEIGHTING = 0.30;

    private static final Double FAVORITED_WEIGHTING = 0.20;

    public static final int REETWEET_SCORE_TRESHOLD = 50;

    private static final int FAVORITE_SCORE_TRESHOLD = 10;

    public static final String RATE = "rate";

    public static void main(final String[] args) throws InterruptedException, ExecutionException, IOException, UnidentifiedUserException {

        Engine engine = new Engine();
        //engine.train("traditional");

        engine.classify("96370324");

        System.exit(0);
    }

    private void classify(String uid) {


        Client client = new Client("V573tdeT4p5sFFcXUwol7yXkflYe2oA93acTVxpeBfdy4w1DU3rdOzPbFXvTHqOg");
        final String bigData = new String(Base64.encodeBase64("tweet".getBytes()));
        String[] attributes = {bigData};
        final ItemRecGetTopNRequestBuilder itemRecGetTopNRequestBuilder =
                client.getItemRecGetTopNRequestBuilder("recommendation-info", uid, 10).itypes(attributes);
        try {
            final String[] itemRecTopN = client.getItemRecTopN(itemRecGetTopNRequestBuilder);
            System.out.println("The result is: " + Arrays.toString(itemRecTopN));
        } catch (ExecutionException | InterruptedException | IOException e) {
            e.printStackTrace();
        }

    }

    public void train(final String rate) {

        File readData = new File("datafile.csv");
        try {
            Client client = new Client("V573tdeT4p5sFFcXUwol7yXkflYe2oA93acTVxpeBfdy4w1DU3rdOzPbFXvTHqOg");

            final List<String> twitterData = FileUtils.readLines(readData);
            twitterData.parallelStream().filter(tweet -> !tweet.equals(FIRST_LINE)).map(tweet -> tweet.split(",")).forEach(tweetSplit -> {

                final String timelineOwner = tweetSplit[1];
                final String isRetweet = tweetSplit[2];
                final String isRetweeted = tweetSplit[3];
                final String retweetCount = tweetSplit[4];
                final String isFavorited = tweetSplit[5];
                final String favoritedCount = tweetSplit[6];
                final String languageCode = tweetSplit[7];
                final String hasTags = tweetSplit[8];
                final String hasMedia = tweetSplit[9];
                final String hasMentions = tweetSplit[10];
                final String hasUrls = tweetSplit[11];

                String timeStamp = format("%s",System.currentTimeMillis());
                if(tweetSplit.length>12) {
                    timeStamp = tweetSplit[12];
                }

                final String retweededStatus = tweetSplit[13];

                List<String> tags = Lists.newArrayList();
                final int length = tweetSplit.length;
                if(length>14){
                    for(int i = 14;i<length;i++){
                        tags.add(tweetSplit[i]);
                    }
                }

                tags.add("tweet");

                // If retweet. Then rate the original tweet!
                final String item = isRetweeted.equals("true") ? retweededStatus : tweetSplit[0];

                createRequestAndRespond(rate,client, item, timelineOwner, isRetweet, isRetweeted, retweetCount, isFavorited, favoritedCount, languageCode, hasTags, hasMedia, hasMentions, hasUrls, tags,timeStamp,retweededStatus);
            });

        } catch (IOException e) {
            log.error("Failed to read data", e);
        }
    }

    private void createRequestAndRespond(String feedbackModel, Client client, String item, String timelineOwner, String isRetweet, String isRetweeted, String retweetCount, String isFavorited, String favoritedCount, String languageCode, String hasTags, String hasMedia, String hasMentions, String hasUrls, List<String> tags, String timeStamp, String retweededStatus) {
        try {

            client.createUser(timelineOwner);
            client.identify(timelineOwner);

            createItem(client, item, timelineOwner, isRetweet, isRetweeted, retweetCount, isFavorited, favoritedCount, languageCode, hasTags, hasMedia, hasMentions, hasUrls, tags,timeStamp);


            if(feedbackModel.equals(RATE)) {
                sendRateFeedback(client, item, timelineOwner, isRetweet, isRetweeted, retweetCount, isFavorited, favoritedCount, languageCode, hasTags, hasMedia, hasMentions, hasUrls);
            } else {
                sendTraditionalFeedback(client, item, timelineOwner, isRetweet, isRetweeted, retweetCount, isFavorited, favoritedCount, languageCode, hasTags, hasMedia, hasMentions, hasUrls,timeStamp,retweededStatus);
            }

        } catch (UnidentifiedUserException | ExecutionException | InterruptedException | NumberFormatException | IOException e) {
            log.error("Failed to add users and action for tweet {}", item, e);
        }
    }

    private void sendTraditionalFeedback(Client client, String item, String timelineOwner, String isRetweet, String isRetweeted, String retweetCount, String isFavorited, String favoritedCount, String languageCode, String hasTags, String hasMedia, String hasMentions, String hasUrls, String timeStamp, String retweededStatus) throws InterruptedException, ExecutionException, IOException, UnidentifiedUserException {

        DateTime dateTime = convertTimeStamp(timeStamp);
        String action = "dislike";

        if(isRetweeted.equals("true")){
            action = UserActionItemRequestBuilder.CONVERSION;
        }


        if(isFavorited.equals("true")){
            action = UserActionItemRequestBuilder.LIKE;
        }

        final UserActionItemRequestBuilder actionItem = client.getUserActionItemRequestBuilder(action, item).t(dateTime);
        client.userActionItem(actionItem);

    }

    private DateTime convertTimeStamp(String timeStamp) {
        return new DateTime(Long.parseLong(timeStamp));
    }

    private void sendRateFeedback(Client client, String item, String timelineOwner, String isRetweet, String isRetweeted, String retweetCount, String isFavorited, String favoritedCount, String languageCode, String hasTags, String hasMedia, String hasMentions, String hasUrls) throws UnidentifiedUserException, ExecutionException, InterruptedException, IOException {
        final Integer rate = calculateRating(item, timelineOwner, timelineOwner, isRetweet, isRetweeted, retweetCount, isFavorited, favoritedCount, languageCode, hasTags, hasMedia, hasMentions, hasUrls);
        System.out.println(format("On item %s is rated %s", item, rate));
        if (rate > 0) {

            final UserActionItemRequestBuilder actionItem = client.getUserActionItemRequestBuilder("rate", item).rate(rate);
            client.userActionItem(actionItem);

        }
    }

    private void createItem(Client client, String item, String timelineOwner, String isRetweet, String isRetweeted, String retweetCount, String isFavorited, String favoritedCount, String languageCode, String hasTags, String hasMedia, String hasMentions, String hasUrls, List<String> tags,String timeStamp) throws ExecutionException, InterruptedException, IOException {
        String[] iTypes = tags.stream().map(String::toLowerCase)
                .map(tag -> encodeAsBase64(tag))
                .collect(Collectors.toList())
                .toArray(new String[tags.size()]);

        final CreateItemRequestBuilder requestBuilder = client.getCreateItemRequestBuilder(item, iTypes)
                .attribute("timelineOwner", timelineOwner)
                .attribute("languageCode", languageCode)
                .attribute("favoriteCount", favoritedCount)
                .attribute("retweetCount", retweetCount)
                .attribute("hasTags", hasTags)
                .attribute("hasMedia", hasMedia)
                .attribute("hasUrls", hasUrls)
                .attribute("timeLineOwner", timelineOwner)
                .attribute("isRetweet", isRetweet)
                .attribute("isRetweeted", isRetweeted)
                .attribute("isFavorited", isFavorited)
                .attribute("hasMentions", hasMentions).startT(convertTimeStamp(timeStamp));


        client.createItem(requestBuilder);
    }

    private String encodeAsBase64(String tag) {
        return new String(encodeBase64(tag.getBytes()));
    }

    private Integer calculateRating(String item, String userId, String timelineOwner, String isRetweet, String isRetweeted, String retweetCount, String isFavorited, String favoritedCount, String languageCode, String hasTags, String hasMedia, String hasMentions, String hasUrls) {

        final Integer maxScore = 100;
        Integer score = 0;

        if (isRetweet.equals("true")) {
            score += 20;
        }


        if (isRetweeted.equals("true")) {
            score += 5;
        }


        final int retweetCountInteger = Integer.parseInt(retweetCount);
        if (retweetCountInteger > 0) {

            Integer retweetScore = 0;
            if (retweetCountInteger > REETWEET_SCORE_TRESHOLD) {
                retweetScore = (int) Math.round(maxScore * REETWEET_WEIGHTING);
            }

            score = score + retweetScore;
        }

        if (isFavorited.equals("true")) {
            score += 0;
        }

        final int favoritedCountInteger = Integer.parseInt(favoritedCount);
        if (favoritedCountInteger > 0) {
            Integer favoriteScore = 0;
            if (favoritedCountInteger > FAVORITE_SCORE_TRESHOLD) {
                favoriteScore = (int) Math.round(maxScore * FAVORITED_WEIGHTING);
            }

            score = score + favoriteScore;
        }


        if (languageCode.equals("en")) {
            score += 15;
        }

        if (hasTags.equals("true")) {
            score += 5;
        }

        if (hasUrls.equals("true")) {
            score += 5;
        }


        final double doublerate = 5.0 * ((double) score / (double) maxScore);
        Integer rate = (int) Math.round(doublerate);

        return rate;

    }

    private static void randomlyRateTweets(Client client, List<String> ratedTweets, List<Long> userUidList) throws ExecutionException, InterruptedException, IOException {
        Random random = new Random();

        for (int i = 0; i < 100000; i++) {
            final Long randomTweetIndex = Math.round(random.nextGaussian() * (ratedTweets.size() * 0.10) + (ratedTweets.size() / 2));
            String ratedTweetItem = ratedTweets.get(randomTweetIndex.intValue());

            final Long randomUserIdIndex = Math.round(random.nextGaussian() * (userUidList.size() * 0.10) + (userUidList.size() / 2));

            final Long userID = userUidList.get(randomUserIdIndex.intValue());

            client.userActionItem(Long.toString(userID), "like", ratedTweetItem);

        }
    }

    private static void retrieveTweetAndRate(Client client, TimelineOperations timelineOperations, List<String> ratedTweets, Long userId) {
        System.out.println("Find a user tweets with id " + userId);
        final List<Tweet> userTweets = timelineOperations.getUserTimeline(userId, 200);
        userTweets.stream().filter(userTweet -> userTweet.isRetweet() || userTweet.isFavorited()).forEach(
                userTweet -> {
                    final String iType = userTweet.isFavorited() ? "favorite" : "retweet";
                    final String[] iTypes = {iType};
                    final String action = userTweet.isFavorited() ? "like" : "conversion";

                    try {
                        final String tweetId = createUserAndRateItem(client, userId, userTweet, iTypes, action);
                        ratedTweets.add(tweetId);
                    } catch (ExecutionException | InterruptedException | IOException e) {
                        System.out.println(e.getMessage());
                    }
                }
        );
    }

    private static String createUserAndRateItem(Client client, Long userId, Tweet userTweet, String[] iTypes, String action) throws ExecutionException, InterruptedException, IOException {
        final String tweetId = userTweet.isFavorited() ? Long.toString(userTweet.getId()) : Long.toString(userTweet.getRetweetedStatus().getId());
        client.createUser(Long.toString(userId));
        final CreateItemRequestBuilder requestBuilder = client.getCreateItemRequestBuilder(tweetId, iTypes)
                .attribute("fromUser", userTweet.getFromUser())
                .attribute("inReplyToScreenName", userTweet.getInReplyToScreenName())
                .attribute("languageCode", userTweet.getLanguageCode())
                .attribute("favoriteCount", userTweet.getFavoriteCount().toString())
                .attribute("retweetCount", userTweet.getRetweetCount().toString())
                .attribute("hasTags", Boolean.toString(userTweet.hasTags()))
                .attribute("hasMedia", Boolean.toString(userTweet.hasMedia()))
                .attribute("hasUrls", Boolean.toString(userTweet.hasUrls()));

        client.createItem(requestBuilder);
        client.userActionItem(Long.toString(userId), action, tweetId);
        return tweetId;
    }

    private static Set<String> createTweetAndScreenNameFromUser(Client client, String screenName, List<Tweet> tweets) {

        Set<String> users = new HashSet<>();

        try {
            client.createUser(screenName);
        } catch (ExecutionException | InterruptedException | IOException e) {
            e.printStackTrace();
        }

        tweets.parallelStream().forEach(
                tweet -> {
                    createTweet(client, tweet);
                    final Long id = tweet.getId();
                    final String action = "conversion";

                    reportActionOnScreenName(client, screenName, id, action);
                    if (tweet.isRetweet()) {
                        final Tweet retweetedStatus = tweet.getRetweetedStatus();
                        final long reetweetUserId = retweetedStatus.getUser().getId();
                        users.add(Long.toString(reetweetUserId));
                        createTweet(client, retweetedStatus);
                        reportActionOnScreenName(client, screenName, retweetedStatus.getId(), "like");
                    }
                }
        );
        return users;
    }

    private static void reportActionOnScreenName(Client client, String screenName, Long id, String action) {
        try {
            client.userActionItem(screenName, action, id.toString());
        } catch (ExecutionException | InterruptedException | IOException e) {
            e.printStackTrace();
        }
    }

    private static void createTweet(Client client, Tweet tweet) {
        final Long id = tweet.getId();

        String[] labels = {"tweet"};
        final CreateItemRequestBuilder requestBuilder = client.getCreateItemRequestBuilder(id.toString(), labels)
                .attribute("fromUser", tweet.getFromUser())
                .attribute("inReplyToScreenName", tweet.getInReplyToScreenName())
                .attribute("languageCode", tweet.getLanguageCode())
                .attribute("favoriteCount", tweet.getFavoriteCount().toString())
                .attribute("retweetCount", tweet.getRetweetCount().toString())
                .attribute("hasTags", Boolean.toString(tweet.hasTags()))
                .attribute("hasMedia", Boolean.toString(tweet.hasMedia()))
                .attribute("hasUrls", Boolean.toString(tweet.hasUrls()));


        try {
            client.createItem(requestBuilder);
        } catch (ExecutionException | IOException | InterruptedException e) {
            System.out.println("Failed to add " + id);
        }
    }

    private static void tokenizeAndCreateItems(Client client, Set<String> stopWords, List<Tweet> tweets, String user) {
        for (Tweet tweet : tweets) {

            final String text = tweet.getText();
            final String[] tokens = text.split(tokenizeRegExp);
            try (java.util.stream.Stream<String> stream = stream(tokens)) {
                stream.filter(a -> {
                    if (!stopWords.contains(a) && !StringUtils.isEmpty(a)) {
                        return true;
                    } else {
                        return false;
                    }
                }).forEach(a -> {
                    createItemOfToken(client, tweet, a);
                    sendConversion(client, a, user);
                });
            }

        }
    }

    private static void sendConversion(Client client, String a, String user) {
        try {
            client.userActionItem(user, "conversion", a);
        } catch (ExecutionException | InterruptedException | IOException e) {
            System.out.println("Failed to  send action. " + a + " why: " + e.getMessage());
        }
    }

    private static void createItemOfToken(Client client, Tweet tweet, String a) {
        String[] labels = {"interesting"};
        final CreateItemRequestBuilder requestBuilder = client.getCreateItemRequestBuilder(a, labels)
                .attribute("fromUser", tweet.getFromUser())
                .attribute("inReplyToScreenName", tweet.getInReplyToScreenName())
                .attribute("languageCode", tweet.getLanguageCode())
                .attribute("favoriteCount", tweet.getFavoriteCount().toString())
                .attribute("retweetCount", tweet.getRetweetCount().toString());

        try {
            client.createItem(requestBuilder);
        } catch (ExecutionException | IOException | InterruptedException e) {
            System.out.println("Failed to add " + a);
        }
    }

}
