package com.example.livy.client;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.livy.LivyClient;
import org.apache.livy.LivyClientBuilder;

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
		if(this.livyClient == null) {
			
			Map<String, String> config = new HashMap<>();
			config.put("spark.app.name", "livy");
			config.put("livy.client.http.connection.timeout", "180s");
			config.put("spark.driver.memory", "1g");
			config.put("spark.sql.crossJoin.enabled", "true");
//			config.put("spark.jars", "/home/davben/.livy-sessions/*");
			String uri = LivyConfigurationManager.getInstance().getProperty("livy.uri");
			livyClient = new LivyClientBuilder(true).setAll(config).setURI(new URI(uri)).build();
			System.out.println("Uploading jars to the SparkContext...");
			LivyClientManager.getInstance().getLivyClient().uploadJar(new File("jars/myJarLight.jar")).get();
			System.out.println("finish uploading jars to the SparkContext...");
		}
		return livyClient;
	}

	public void reset() {
		instance = null;
	}
	
}
