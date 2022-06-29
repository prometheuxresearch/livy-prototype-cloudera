package com.example.livy.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class LivyConfigurationManager {

	public static final String LIVY_CONF_FILE = "livy.properties";
	private static LivyConfigurationManager lcm = null;
	private final Properties configProp = new Properties();

	private LivyConfigurationManager() {
	}

	public static LivyConfigurationManager getInstance() throws Exception {
		if (lcm == null) {
			lcm = new LivyConfigurationManager();
			lcm.loadProperties();
		}
		return lcm;
	}

	public void loadProperties() throws Exception {
		InputStream in = null;
		File external = new File(LIVY_CONF_FILE);
		Properties configPropExternal = new Properties();
		if (external.exists()) {
			try {
				in = new FileInputStream(external);
				configPropExternal.load(in);
			} catch (FileNotFoundException e) {
				throw new Exception(e.getMessage());
			} catch (IOException e) {
				throw new Exception(e.getMessage());
			}
		}
		in = this.getClass().getClassLoader().getResourceAsStream(LIVY_CONF_FILE);
		try {
			configProp.load(in);
			configProp.putAll(configPropExternal);
		} catch (IOException e) {
			throw new Exception(e.getMessage());
		}

		configPropExternal = new Properties();
		if (configPropExternal.size() > 0)
			configProp.putAll(configPropExternal);
	}

	public void loadProperties(Properties configProp) {
		this.configProp.clear();
		this.configProp.putAll(configProp);
	}

	public String getProperty(String key, String def) {
		if (configProp.containsKey(key))
			return configProp.getProperty(key);
		return def;
	}

	public String getProperty(String key) {
		return getProperty(key, null);
	}

	public void reset() {
		lcm = null;
	}

}
