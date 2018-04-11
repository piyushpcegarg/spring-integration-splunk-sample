package com.gargorg.splunksample.config;

import com.gargorg.splunksample.service.SplunkServiceFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.EndpointId;
import org.springframework.integration.annotation.InboundChannelAdapter;
import org.springframework.integration.annotation.Poller;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.splunk.core.DataReader;
import org.springframework.integration.splunk.core.ServiceFactory;
import org.springframework.integration.splunk.event.SplunkEvent;
import org.springframework.integration.splunk.inbound.SplunkPollingChannelAdapter;
import org.springframework.integration.splunk.support.SearchMode;
import org.springframework.integration.splunk.support.SplunkDataReader;
import org.springframework.integration.splunk.support.SplunkExecutor;
import org.springframework.integration.splunk.support.SplunkServer;
import org.springframework.integration.stream.CharacterStreamWritingMessageHandler;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;

import java.util.List;

@Configuration
@EnableIntegration
public class SplunkConfiguration {

    @Value("${splunkserver.host}")
    private String host;
    @Value("${splunkserver.port}")
    private int port;
    @Value("${splunkserver.scheme}")
    private String scheme;
    @Value("${splunkserver.app}")
    private String app;

    @Value("${splunkserver.owner}")
    private String owner;
    @Value("${splunkserver.username}")
    private String username;
    @Value("${splunkserver.password}")
    private String password;

    @Value("${splunkdatareader.mode}")
    private SearchMode mode;
    @Value("${splunkdatareader.search}")
    private String serach;
    @Value("${splunkdatareader.earliestTime}")
    private String earliestTime;
    @Value("${splunkdatareader.latestTime}")
    private String latestTime;
    @Value("${splunkdatareader.initEarliestTime}")
    private String initEarliestTime;

    @Bean
    public SplunkServer splunkServer() {

        SplunkServer splunkServer = new SplunkServer();

        splunkServer.setHost(host);
        splunkServer.setPort(port);
        splunkServer.setScheme(scheme);
        splunkServer.setApp(app);

        splunkServer.setOwner(owner);
        splunkServer.setUsername(username);
        splunkServer.setPassword(password);

        return splunkServer;
    }

    @Bean
    public DataReader dataReader() {

        SplunkDataReader splunkDataReader = new SplunkDataReader(serviceFactory());
        splunkDataReader.setMode(mode);
        splunkDataReader.setSearch("search "+ serach);
        splunkDataReader.setEarliestTime(earliestTime);
        splunkDataReader.setLatestTime(latestTime);
        splunkDataReader.setInitEarliestTime(initEarliestTime);

        return splunkDataReader;
    }

    @Bean
    public SplunkExecutor splunkExecutor() {

        SplunkExecutor splunkExecutor = new SplunkExecutor();

        splunkExecutor.setReader(dataReader());
        return splunkExecutor;
    }

    @Bean(name = "inputFromSplunk")
    public MessageChannel splunkChannel() {

        return new DirectChannel();
    }

    @Bean
    @EndpointId("splunkInboundChannelAdapter")
    @InboundChannelAdapter(channel = "inputFromSplunk", poller = @Poller(fixedRate = "300000"))
    public MessageSource<List<SplunkEvent>> splunkPollingChannelAdapter() {

        SplunkPollingChannelAdapter splunkPollingChannelAdapter = new SplunkPollingChannelAdapter(splunkExecutor());

        return splunkPollingChannelAdapter;
    }

    @Bean
    public ServiceFactory serviceFactory() {

        return new SplunkServiceFactory(splunkServer());
    }

    @Bean
    @ServiceActivator(inputChannel = "inputFromSplunk")
    public MessageHandler characterStreamWritingMessageHandler() {

        CharacterStreamWritingMessageHandler characterStreamWritingMessageHandler =
                CharacterStreamWritingMessageHandler.stdout();

        characterStreamWritingMessageHandler.setShouldAppendNewLine(true);

        return characterStreamWritingMessageHandler;
    }
}