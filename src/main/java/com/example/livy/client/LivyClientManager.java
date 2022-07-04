package com.example.livy.client;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import com.cloudera.livy.LivyClient;
import com.cloudera.livy.LivyClientBuilder;
import com.example.livy.common.LivyConfigurationManager;

public class LivyClientManager {
	
	private static LivyClientManager instance;
	private LivyClient livyClient;

	
	public static LivyClientManager getInstance() throws IOException, URISyntaxException, InterruptedException, ExecutionException {
		if (instance == null) {
			instance = new LivyClientManager();
		}
		return instance;
	}
	
	public LivyClient getLivyClient() throws Exception {
		if (this.livyClient == null) {
//			String clouderaClientId = LivyConfigurationManager.getInstance().getProperty("livy.client.auth.id");
//			String clouderaClientSecret = LivyConfigurationManager.getInstance().getProperty("livy.client.auth.secret");
			Map<String, String> config = new HashMap<>();
//			config.put("spark.app.name", "livy");
//			config.put("livy.client.http.connection.timeout", "180s");
//			config.put("spark.driver.memory", "1g");
			config.put("spark.sql.crossJoin.enabled", "true");
//			config.put("client.auth.id", clouderaClientId);
//			config.put("client.auth.secret", clouderaClientSecret);
			
			String javaSecurityAuthLoginConfig = LivyConfigurationManager.getInstance().getProperty("java.security.auth.login.config");
			System.setProperty("java.security.auth.login.config", javaSecurityAuthLoginConfig);
			
			String javaSecurityKrb5Conf = LivyConfigurationManager.getInstance().getProperty("java.security.krb5.conf");
			System.setProperty("java.security.krb5.conf", javaSecurityKrb5Conf);
			
			String sunSecurityKrb5Debug = LivyConfigurationManager.getInstance().getProperty("sun.security.krb5.debug");
			System.setProperty("sun.security.krb5.debug", sunSecurityKrb5Debug);
			
			String javaSecurityAuthUseSubjectCredsOnly = LivyConfigurationManager.getInstance().getProperty("javax.security.auth.useSubjectCredsOnly");
			System.setProperty("javax.security.auth.useSubjectCredsOnly", javaSecurityAuthUseSubjectCredsOnly);

//			config.put("spark.jars", "/home/davben/.livy-sessions/*");
			String uri = LivyConfigurationManager.getInstance().getProperty("livy.uri");
			livyClient = new LivyClientBuilder(false).setAll(config).setURI(new URI(uri)).build();
			System.out.println("Uploading jars to the SparkContext...");
			File jarFile = new File("myJarLight.jar");
			livyClient.uploadJar(jarFile).get();
			System.out.println("finish uploading jars to the SparkContext...");
		}
		return livyClient;
	}

	public void reset() {
		instance = null;
	}
	
}
