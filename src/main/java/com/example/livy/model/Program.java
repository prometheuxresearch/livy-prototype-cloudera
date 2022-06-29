package com.example.livy.model;

public class Program {
	
	public Program() {
		
	}
	public Program(String content, String outputDir) {
		this.programContent = content;
		this.outputDir = outputDir;
	}
	
	private String programContent = "";
	private String outputDir = "";

	public String getProgramContent() {
		return programContent;
	}

	public void setProgramContent(String programContent) {
		this.programContent = programContent;
	}

	public String getOutputDir() {
		return outputDir;
	}

	public void setOutputDir(String outputDir) {
		this.outputDir = outputDir;
	}
	
	

}
