package com.example.livy;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutionException;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


@SpringBootApplication
public class LivyApplication {

	public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException, ExecutionException {
		SpringApplication.run(LivyApplication.class, args);
	}
	
}
