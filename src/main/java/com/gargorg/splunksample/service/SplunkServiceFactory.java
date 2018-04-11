package com.gargorg.splunksample.service;

import com.splunk.SSLSecurityProtocol;
import com.splunk.Service;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.integration.splunk.core.ServiceFactory;
import org.springframework.integration.splunk.support.SplunkServer;
import org.springframework.util.Assert;

import javax.xml.bind.DatatypeConverter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * A {@link FactoryBean} for creating a {@link Service}
 *
 */
public class SplunkServiceFactory implements ServiceFactory {

    private static final Log LOGGER = LogFactory.getLog(SplunkServiceFactory.class);

    private final List<SplunkServer> splunkServers;

    private final Map<SplunkServer, Service> servicePerServer = new ConcurrentHashMap<SplunkServer, Service>();

    /**
     * @param splunkServer the {@code SplunkServer} to build this {@code SplunkServiceFactory}
     * @since 1.1
     */
    public SplunkServiceFactory(SplunkServer splunkServer) {
        Assert.notNull(splunkServer, "splunkServer must not be null");
        this.splunkServers = Arrays.asList(splunkServer);
    }

    @Override
    public synchronized Service getService() {
        return getServiceInternal();
    }

    private Service getServiceInternal() {

        for (SplunkServer splunkServer : splunkServers) {
            Service service = servicePerServer.get(splunkServer);
            // service already exist and no test on borrow it so simply use it

            if (service != null) {
                if (!splunkServer.isCheckServiceOnBorrow() || pingService(service)) {
                    return service;
                }
                else {
                    // fail so try next server
                    continue;
                }
            }

            ExecutorService executor = Executors.newSingleThreadExecutor();

            Callable<Service> callable = buildServiceCallable(splunkServer);

            Future<Service> future = executor.submit(callable);

            try {
                if (splunkServer.getTimeout() > 0) {
                    service = future.get(splunkServer.getTimeout(), TimeUnit.MILLISECONDS);
                }
                else {
                    service = future.get();
                }

                servicePerServer.put(splunkServer, service);
                return service;
            }
            catch (Exception e) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info(String.format("could not connect to Splunk Server @ %s:%d - %s, try next one",
                            splunkServer.getHost(), splunkServer.getPort(), e.getMessage()));
                }
            }
        }
        String message = String.format("could not connect to any of Splunk Servers %s", this.splunkServers);
        LOGGER.error(message);
        throw new RuntimeException(message);
    }

    private Callable<Service> buildServiceCallable(SplunkServer splunkServer) {
        final Map<String, Object> args = new HashMap<String, Object>();
        if (splunkServer.getHost() != null) {
            args.put("host", splunkServer.getHost());
        }
        if (splunkServer.getPort() != 0) {
            args.put("port", splunkServer.getPort());
        }
        if (splunkServer.getScheme() != null) {
            args.put("scheme", splunkServer.getScheme());
        }
        if (splunkServer.getApp() != null) {
            args.put("app", splunkServer.getApp());
        }
        if (splunkServer.getOwner() != null) {
            args.put("owner", splunkServer.getOwner());
        }

        args.put("username", splunkServer.getUsername());
        args.put("password", splunkServer.getPassword());

        String auth = splunkServer.getUsername() + ":" + splunkServer.getPassword();
        String authToken = "Basic " + DatatypeConverter.printBase64Binary(auth.getBytes());
        args.put("token", authToken);

        args.put("SSLSecurityProtocol", SSLSecurityProtocol.TLSv1_2);

        return new Callable<Service>() {
            public Service call()
                    throws Exception {
                return Service.connect(args);
            }
        };
    }

    private boolean pingService(Service service) {
        try {
            service.getInfo();
            return true;
        }
        catch (Exception e) {
            return false;
        }
    }
}
