package io.monokkel.recommendation;

import com.google.common.collect.Lists;
import io.prediction.*;
import org.apache.commons.cli.*;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.apache.commons.codec.binary.Base64.encodeBase64;

public class Engine {

    private static final Logger log = LogManager.getLogger(Engine.class);

    public static final String FIRST_LINE = "tweetId,timelineOwner,isRetweet,isRetweeted,retweetCount,isFavorited,favoritedCount,languageCode,hasTags,hasMedia,hasMentions,hasUrls";

    public static final String ENGINE_MODE_TRAIN = "train";

    public static final String ENGINE_MODE_RECOMMEND = "recommend";

    public static final int RECOMMENDATIONS_TO_RECEIVE = 10;

    public static final String TRUE = "true";

    private Client client;

    public static void main(final String[] args) {

        Engine engine = new Engine();
        engine.setClient(new Client("")); // Empty string here. The appKey is set from the command line
        final Integer exitCode = engine.start(args);
        System.exit(exitCode);
    }

    public Integer start(String[] args) {

        CommandLineParser commandLineParser = new GnuParser();
        Options options = createOptions();

        final CommandLine parsedCommandLine = parseCommandLine(args, commandLineParser, options);
        final String mode = parsedCommandLine.getOptionValue("mode");

        final String appKey = parsedCommandLine.getOptionValue("appKey");
        client.setAppkey(appKey);

        final String engineName = parsedCommandLine.getOptionValue("engineName");

        switch (mode) {
            case ENGINE_MODE_TRAIN:
                if (parsedCommandLine.hasOption("dataFile")) {
                    final String dataFile = parsedCommandLine.getOptionValue("dataFile");
                    trainWithDataFromDile(dataFile);
                   return 0;
                } else {
                    log.error("No dataFile parameter found in the command input");
                    printCommandHelp(options);
                    return 1;
                }
            case ENGINE_MODE_RECOMMEND:
                if (parsedCommandLine.hasOption("uid")) {
                    final String uid = parsedCommandLine.getOptionValue("uid");
                    this.recommend(uid, engineName);
                    return 0;

                } else {
                    log.error("No uid parameter found in the command input");
                    printCommandHelp(options);
                    return 1;
                }

            default:
                printCommandHelp(options);
                return 1;
        }

    }

    private void trainWithDataFromDile(final String dataFile) {
        List<String> twitterData = null;
        try {
            File readData = new File(dataFile);
            twitterData = FileUtils.readLines(readData);
        } catch (FileNotFoundException f) {
            log.warn("Could not find the file {}. Trying to find it on classpath", dataFile);
            twitterData = getTwitterDataFromClassPath(dataFile);
        } catch (IOException e) {
            log.error("Failed to read file {}", dataFile, e);
            System.exit(1);
        }

        this.train(twitterData);
    }

    private List<String> getTwitterDataFromClassPath(final String dataFile) {
        try {
            final InputStream resourceAsStream = Engine.class.getResourceAsStream('/' + dataFile);
            return IOUtils.readLines(resourceAsStream);
        } catch (Exception e) {
            log.error("Failed to read file {}", dataFile, e);
            System.exit(1);
            return Lists.newArrayList();
        }
    }

    private CommandLine parseCommandLine(String[] args, CommandLineParser commandLineParser, Options options) {
        CommandLine parsedCommandLine = null;
        try {
            parsedCommandLine = commandLineParser.parse(options, args);
        } catch (ParseException e) {
            printCommandHelp(options);
            System.exit(1);
        }
        return parsedCommandLine;
    }

    private void printCommandHelp(Options options) {
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.printHelp(" -mode recommend | train -appKey -engineName <PredictionIO engine name> [-dataFile <filepath>]  [-uid [0-9]+]", options);
    }


    @SuppressWarnings("static-access")
    private static Options createOptions() {
        Options options = new Options();
        Option mode = OptionBuilder.withDescription("Select mode of the recommendation engine. mode: train | recommend").hasArg(true).isRequired().create("mode");
        options.addOption(mode);

        final Option uid = OptionBuilder.withDescription("Supply an twitter UID  you want to get recommendations for").hasArg(true).create("uid");
        options.addOption(uid);

        final Option appKey = OptionBuilder.hasArg().isRequired().create("appKey");
        options.addOption(appKey);

        final Option dataFile = OptionBuilder.hasArg().create("dataFile");
        options.addOption(dataFile);


        final Option engineName = OptionBuilder.hasArg().isRequired().create("engineName");
        options.addOption(engineName);


        return options;
    }

    private void recommend(final String uid, final String engineName) {

        final String bigData = new String(Base64.encodeBase64("tweet".getBytes()));
        String[] attributes = {bigData};
        final ItemRecGetTopNRequestBuilder itemRecGetTopNRequestBuilder =
                client.getItemRecGetTopNRequestBuilder(engineName, uid, RECOMMENDATIONS_TO_RECEIVE).itypes(attributes);
        try {
            final String[] itemRecTopN = client.getItemRecTopN(itemRecGetTopNRequestBuilder);
            log.info("The result is: {}", Arrays.toString(itemRecTopN));
        } catch (ExecutionException | InterruptedException | IOException e) {
            log.error("Failed to recommend uid {}", uid, e);
        }

    }

    public void train(final List<String> twitterData) {

        twitterData.parallelStream().filter(tweet -> !tweet.contains(FIRST_LINE)).map(tweet -> tweet.split(",")).forEach(tweetSplit -> {

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

            String timeStamp = format("%s", System.currentTimeMillis());
            if (tweetSplit.length > 12) {
                timeStamp = tweetSplit[12];
            }

            final String retweetedStatus = tweetSplit[13];

            List<String> tags = Lists.newArrayList();
            final int length = tweetSplit.length;
            if (length > 14) {
                tags.addAll(asList(tweetSplit).subList(14, length));
            }

            tags.add("tweet");

            // If retweet. Then rate the original tweet!
            final String item = isRetweeted.equals("true") ? retweetedStatus : tweetSplit[0];

            createRequestAndRespond(client, item, timelineOwner, isRetweet, isRetweeted, retweetCount, isFavorited, favoritedCount, languageCode, hasTags, hasMedia, hasMentions, hasUrls, tags, timeStamp, retweetedStatus);
        });
    }

    private void createRequestAndRespond(Client client, String item, String timelineOwner, String isRetweet, String isRetweeted, String retweetCount, String isFavorited, String favoritedCount, String languageCode, String hasTags, String hasMedia, String hasMentions, String hasUrls, List<String> tags, String timeStamp, String retweededStatus) {
        try {

            client.createUser(timelineOwner);

            createItem(client, item, timelineOwner, isRetweet, isRetweeted, retweetCount, isFavorited, favoritedCount, languageCode, hasTags, hasMedia, hasMentions, hasUrls, tags, timeStamp);

            sendFeedback(client, item, timelineOwner, isRetweet, isRetweeted, retweetCount, isFavorited, favoritedCount, languageCode, hasTags, hasMedia, hasMentions, hasUrls, timeStamp, retweededStatus);

        } catch (UnidentifiedUserException | ExecutionException | InterruptedException | NumberFormatException | IOException e) {
            log.error("Failed to add users and action for tweet {}", item, e);
        }
    }

    private void sendFeedback(Client client, String item, String timelineOwner, String isRetweet, String isRetweeted, String retweetCount, String isFavorited, String favoritedCount, String languageCode, String hasTags, String hasMedia, String hasMentions, String hasUrls, String timeStamp, String retweededStatus) throws InterruptedException, ExecutionException, IOException, UnidentifiedUserException {

        DateTime dateTime = convertTimeStamp(timeStamp);
        final String action = selectAction(isRetweeted, isFavorited);

        final UserActionItemRequestBuilder userActionItemRequestBuilder = client.getUserActionItemRequestBuilder(timelineOwner, action, item).t(dateTime);
        client.userActionItem(userActionItemRequestBuilder);

    }

    private String selectAction(String isRetweeted, String isFavorited) {
        String action;

        if (isRetweeted.equals(TRUE)) {
            action = UserActionItemRequestBuilder.CONVERSION;
        } else if (isFavorited.equals(TRUE)) {
            action = UserActionItemRequestBuilder.LIKE;
        } else {
            action = UserActionItemRequestBuilder.DISLIKE;
        }
        return action;
    }

    private DateTime convertTimeStamp(String timeStamp) {
        return new DateTime(Long.parseLong(timeStamp));
    }


    private void createItem(Client client, String item, String timelineOwner, String isRetweet, String isRetweeted, String retweetCount, String isFavorited, String favoritedCount, String languageCode, String hasTags, String hasMedia, String hasMentions, String hasUrls, List<String> tags, String timeStamp) throws ExecutionException, InterruptedException, IOException {
        String[] iTypes = tags.stream().map(String::toLowerCase)
                .map(this::encodeAsBase64)
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

    public void setClient(Client client) {
        this.client = client;
    }


}
