package com.glocks.constants;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.PropertySources;
import org.springframework.stereotype.Component;

@Component

@PropertySources({
        @PropertySource(
                value = {"file:application.properties"},
                ignoreResourceNotFound = true),
        @PropertySource(
                value = {"file:configuration.properties"},
                ignoreResourceNotFound = true)
})
public class PropertiesReader {

    @Value("${appdbName}")
    public String appdbName;

    @Value("${repdbName}")
    public String repdbName;

    @Value("${auddbName}")
    public String auddbName;

    @Value("${serverName}")
    public String serverName;

    @Value("${comma-delimitor}")
    public String commaDelimiter;

    @Value("${localMsisdnStartSeries}")
    public String localMsisdnStartSeries;

    @Value("${localISMIStartSeries}")
    public String localISMIStartSeries;

    @Value("${sqlInputPath}")
    public String sqlInputPath;

    @Value("${p3ProcessedPath}")
    public String p3ProcessedPath;

    @Value("${sleepTime}")
    public String sleepTime;

}
