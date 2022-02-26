package es.udc.fi.ri.mri_indexer.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public final class ConfigurationParametersManager {

    private static final String CONFIGURATION_FILE = "ConfigurationParameters.properties";
    private static Map<String, String> parameters;

    private ConfigurationParametersManager() {
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static synchronized Map<String, String> getParameters() {

        if (parameters == null) {
            Class<ConfigurationParametersManager> 
                    configurationParametersManagerClass = 
                    ConfigurationParametersManager.class;
            ClassLoader classLoader = 
                    configurationParametersManagerClass.getClassLoader();
            InputStream inputStream = 
                    classLoader.getResourceAsStream(CONFIGURATION_FILE);
            Properties properties = new Properties();
            try {
                properties.load(inputStream);
                inputStream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            /*
             * We use a "HashMap" instead of a "HashTable" because HashMap's
             * methods are *not* synchronized (so they are faster), and the
             * parameters are only read.
             */
            parameters = (Map<String, String>) new HashMap(properties);

        }
        return parameters;

    }

    public static String getParameter(String name) {

        return getParameters().get(name);

    }
}
