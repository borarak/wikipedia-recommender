package config;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

/**
 * Configuration File Reader and called in static context
 * @author raktim
 * @version 1.0
 *
 */

@SuppressWarnings("rawtypes")
public class ConfigProperties {
    private static Properties initialProp; 
	private static Set keys;

    public ConfigProperties(){
        initialProp = new Properties();
        FileInputStream in;
        try {
        	ClassLoader classLoader = getClass().getClassLoader();
            in = new FileInputStream(classLoader.getResource("Config.properties").getFile());
            initialProp.load(in);
            in.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        keys = initialProp.keySet();
        Iterator iter = keys.iterator();
        while(iter.hasNext()){
        	String temp = (String) iter.next();
        }        
    }
    
    public static boolean containsProperty(String prop){
        if(keys.contains(prop)){
            return true;
        }
        else
            return false;
    }
    
    
    /**
     * Returns the value of a configured property
     * @param propertyName - name of property
     * @return - value of property
     */
    public String getProperty(String propertyName){
        if(!containsProperty(propertyName)){
            System.out.println("Encountered a non-existing configuration:" + propertyName);
            quit();
        }
        return (String) initialProp.get(propertyName);        
    }

    private static void quit(){
        System.out.println("Missing some configurations. Exiting!");
    }
}



