package com.glocks.configuration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.PropertySources;
import org.springframework.stereotype.Component;

@Component
// @PropertySource("classpath:application.properties")

@PropertySources({
        @PropertySource(value = {"file:application.properties"}, ignoreResourceNotFound = true),
        @PropertySource(value = {"file:configuration.properties"}, ignoreResourceNotFound = true)
})
public class PropertiesReader {

    @Value("${appdbName}")
    public String appdbName;

    @Value("${edrappdbName}")
    public String edrappdbName;

    @Value("${repdbName}")
    public String repdbName;

    @Value("${auddbName}")
    public String auddbName;

    @Value("${oamdbName}")
    public String oamdbName;

    @Value("${serverName}")
    public String serverName;

    @Value("${comma-delimitor}")
    public String commaDelimiter;

    @Value("${localMsisdnStartSeries}")
    public String localMsisdnStartSeries;

    @Value("${localISMIStartSeries}")
    public String localISMIStartSeries;

}
