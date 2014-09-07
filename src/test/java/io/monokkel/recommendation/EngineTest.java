package io.monokkel.recommendation;

import io.prediction.Client;
import io.prediction.CreateItemRequestBuilder;
import io.prediction.ItemRecGetTopNRequestBuilder;
import io.prediction.UserActionItemRequestBuilder;
import org.joda.time.DateTime;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.mockito.Mockito.*;

public class EngineTest {


    @Test
    public void start_withRecommendFlag_expectClientToBeCalledWithRequestBuilder() throws InterruptedException, ExecutionException, IOException {

        Engine engine = new Engine();
        final Client mockedClient = Mockito.mock(Client.class);

        ItemRecGetTopNRequestBuilder mockedItemRequestBuilder = Mockito.mock(ItemRecGetTopNRequestBuilder.class);
        final String engineName = "recommendation-info";
        final String uid = "96370324";
        when(mockedClient.getItemRecGetTopNRequestBuilder(engineName, uid, Engine.RECOMMENDATIONS_TO_RECEIVE)).thenReturn(mockedItemRequestBuilder);
        when(mockedClient.getItemRecTopN(any(ItemRecGetTopNRequestBuilder.class))).thenReturn(new String[]{"1234", "1234"});

        engine.setClient(mockedClient);
        engine.start(new String[]{"-mode", Engine.ENGINE_MODE_RECOMMEND, "-uid", uid, "-appKey", "V573tdeT4p5sFFcXUwol7yXkflYe2oA93acTVxpeBfdy4w1DU3rdOzPbFXvTHqOg", "-engineName", engineName});

        verify(mockedClient, times(1)).getItemRecTopN(any(ItemRecGetTopNRequestBuilder.class));

    }

    @Test
    public void start_withTrainFlag_expectCreateItemToBeCalled() throws InterruptedException, ExecutionException, IOException {

        Engine engine = new Engine();
        final Client mockedClient = Mockito.mock(Client.class);

        final String engineName = "recommendation-info";
        CreateItemRequestBuilder mockedItemRequestBuilder = setupMockedItemRequestBuilder(mockedClient);
        setupUserActionRequestBuilder(mockedClient);

        engine.setClient(mockedClient);
        engine.start(new String[]{"-mode", Engine.ENGINE_MODE_TRAIN, "-appKey", "V573tdeT4p5sFFcXUwol7yXkflYe2oA93acTVxpeBfdy4w1DU3rdOzPbFXvTHqOg", "-engineName", engineName, "-dataFile", "testDataFile.csv"});


        verify(mockedClient, times(1)).createItem(mockedItemRequestBuilder);
    }

    @Test
    public void start_withTrainFlag_expectCreateUserToBeCalled() throws InterruptedException, ExecutionException, IOException {

        Engine engine = new Engine();
        final Client mockedClient = Mockito.mock(Client.class);

        final String engineName = "recommendation-info";
        setupMockedItemRequestBuilder(mockedClient);
        final UserActionItemRequestBuilder mockedUserActionRequestBuilder = setupUserActionRequestBuilder(mockedClient);

        engine.setClient(mockedClient);
        engine.start(new String[]{"-mode", Engine.ENGINE_MODE_TRAIN, "-appKey", "V573tdeT4p5sFFcXUwol7yXkflYe2oA93acTVxpeBfdy4w1DU3rdOzPbFXvTHqOg", "-engineName", engineName, "-dataFile", "testDataFile.csv"});
        verify(mockedClient, times(1)).userActionItem(mockedUserActionRequestBuilder);

    }


    @Test
    public void start_withTrainFlag_expectUserActionToBeCalled() throws InterruptedException, ExecutionException, IOException {

        Engine engine = new Engine();
        final Client mockedClient = Mockito.mock(Client.class);

        final String engineName = "recommendation-info";
        setupMockedItemRequestBuilder(mockedClient);
        setupUserActionRequestBuilder(mockedClient);

        engine.setClient(mockedClient);
        engine.start(new String[]{"-mode", Engine.ENGINE_MODE_TRAIN, "-appKey", "V573tdeT4p5sFFcXUwol7yXkflYe2oA93acTVxpeBfdy4w1DU3rdOzPbFXvTHqOg", "-engineName", engineName, "-dataFile", "testDataFile.csv"});

        verify(mockedClient, times(1)).createUser(anyString());

    }


    private UserActionItemRequestBuilder setupUserActionRequestBuilder(Client mockedClient) {
        UserActionItemRequestBuilder mockedUserActionRequestBuilder = mock(UserActionItemRequestBuilder.class);
        when(mockedClient.getUserActionItemRequestBuilder(anyString(), anyString(), anyString())).thenReturn(mockedUserActionRequestBuilder);
        when(mockedUserActionRequestBuilder.t(any(DateTime.class))).thenReturn(mockedUserActionRequestBuilder);
        return mockedUserActionRequestBuilder;
    }

    private CreateItemRequestBuilder setupMockedItemRequestBuilder(Client mockedClient) throws ExecutionException, InterruptedException, IOException {
        CreateItemRequestBuilder mockedItemRequestBuilder = Mockito.mock(CreateItemRequestBuilder.class);
        when(mockedClient.getCreateItemRequestBuilder(anyString(), anyVararg())).thenReturn(mockedItemRequestBuilder);
        when(mockedItemRequestBuilder.attribute(anyString(), anyString())).thenReturn(mockedItemRequestBuilder)
                .thenReturn(mockedItemRequestBuilder)
                .thenReturn(mockedItemRequestBuilder)
                .thenReturn(mockedItemRequestBuilder)
                .thenReturn(mockedItemRequestBuilder)
                .thenReturn(mockedItemRequestBuilder)
                .thenReturn(mockedItemRequestBuilder)
                .thenReturn(mockedItemRequestBuilder)
                .thenReturn(mockedItemRequestBuilder)
                .thenReturn(mockedItemRequestBuilder)
                .thenReturn(mockedItemRequestBuilder)
                .thenReturn(mockedItemRequestBuilder)
                .thenReturn(mockedItemRequestBuilder);
        when(mockedItemRequestBuilder.startT(any(DateTime.class))).thenReturn(mockedItemRequestBuilder);
        when(mockedClient.getItemRecTopN(any(ItemRecGetTopNRequestBuilder.class))).thenReturn(new String[]{"1234", "1234"});
        return mockedItemRequestBuilder;
    }


}