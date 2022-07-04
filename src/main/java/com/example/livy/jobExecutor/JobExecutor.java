package com.example.livy.jobExecutor;

import com.cloudera.livy.LivyClient;
import com.example.livy.client.LivyClientManager;
import com.example.livy.job.MyJob;

public class JobExecutor {

	public static boolean execute(MyJob job)
			throws Exception {
		LivyClient livyClient = LivyClientManager.getInstance().getLivyClient();
		Boolean res = false;

		try {
			res = livyClient.submit(job).get();
		} finally {
			livyClient.stop(true);
			LivyClientManager.getInstance().reset();
		}
		return res;
	}

}
