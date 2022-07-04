package com.example.livy.job;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import com.cloudera.livy.Job;
import com.cloudera.livy.JobContext;

public class MyJob implements Job<Boolean>{
	String program = "";
	String myDir = "";
	
	public MyJob(String program, String myDir) {
		this.program = program;
		this.myDir = myDir;
	}
	
	public MyJob() {
		
	}

	private static final long serialVersionUID = 1L;

	@Override
	public Boolean call(JobContext jc) throws Exception {
		SparkSession spark = jc.sqlctx().sparkSession();
		String[] headAndBody = this.program.split(",");
		if(headAndBody.length == 2) {
			this.doJoin(spark);
		}
		else {
			this.doLinear(spark);
		}
		return true;
	}

	private void doLinear(SparkSession spark) {
		List<Row> rows = new ArrayList<>();
		rows.add(RowFactory.create(0.1, "b"));
		rows.add(RowFactory.create(0.2, "c"));
		rows.add(RowFactory.create(0.3, "d"));
		rows.add(RowFactory.create(0.4, "b"));

		StructType structType = new StructType();
		structType = structType.add("edge_1", DataTypes.DoubleType, false);
		structType = structType.add("edge_2", DataTypes.StringType, false);
		ExpressionEncoder<Row> edgeEncoder = RowEncoder.apply(structType);

		Dataset<Row> edgeA = spark.createDataset(rows, edgeEncoder);
		Dataset<Row> edgeB = edgeA.select(new Column("edge_1").as("edge_11"), 
				new Column("edge_2").as("edge_22"));
		
		edgeB.write().mode(SaveMode.Overwrite).csv(myDir+"edgeB.csv");
		
	}

	private void doJoin(SparkSession spark) {
		List<Row> rows = new ArrayList<>();
		rows.add(RowFactory.create(0.1, "b"));
		rows.add(RowFactory.create(0.2, "c"));
		rows.add(RowFactory.create(0.3, "d"));
		rows.add(RowFactory.create(0.4, "b"));

		StructType structType = new StructType();
		structType = structType.add("edge_1", DataTypes.DoubleType, false);
		structType = structType.add("edge_2", DataTypes.StringType, false);
		ExpressionEncoder<Row> edgeEncoder = RowEncoder.apply(structType);

		Dataset<Row> edgeA = spark.createDataset(rows, edgeEncoder);
		Dataset<Row> edgeB = edgeA.select(new Column("edge_1").as("edge_11"), 
				new Column("edge_2").as("edge_22"));
		Dataset<Row> join = edgeA.as("left").join(edgeB.as("right"));
		
		join.write().mode(SaveMode.Overwrite).csv(myDir+"join.csv");
	}

}
