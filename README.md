tweet-recommendation
====================

This is a very experimental app.. be careful.

# Download tweets

No cmd support yet... you need to edit the code

* Register your own Twitter App here: https://apps.twitter.com/
* Create an access token under the API Keys tab
* cd tweet-recommendation
* Open class io.monokkel.recommendation.DataDownloader
* Edit the variables with the correct keys from the web page. (API key == consumerKey and API secret == consumerSecret) 
* Optional: Change the path of the datafile csv
* Save and run it either with your ide or by using maven: 
```mvn clean compile  exec:java -X -Dexec.mainClass="io.monokkel.recommendation.DataDownloader"```
* This would normally take a while to download all your friends last 200 tweets. It will hit a few request limits here and there and trigger a sleep.  
* If you follow few accounts, it would be beneficially to get more than 200 tweets per user. Please hack the DataDownloader class!

# Interact with PredictionIO

* Install PredictionIO at localhost: http://docs.prediction.io/current/installation/index.html
* Create an application and engine
* ```cd tweet-recommendation```
* ```Run mvn clean package```
* ```chmod 755 target/appassembler/bin/app```

## Train/Upload data

* ```target/appassembler/bin/app -mode train -appKey <PredictionIO appKey> -dataFile <Full or relative path>``` 
* Check the app page in the PredictionIO admin UI
* Then find an algorithm and train it! (The difficult part)

## Test recommendation

* Find the Twitter id of your user. Hint: Look in the second column in the top of the data csv file
* ```target/appassembler/bin/app -mode recommend -appKey <PredictionIO appKey> -engineName <PredictionIO engine name> -uid <The twitter user id (unsigned integer)>```
* Read the super fancy output printed to the screen. It is a list of twitter ids. To see the actual tweet: http://twitter.com/statuses/long_id_here