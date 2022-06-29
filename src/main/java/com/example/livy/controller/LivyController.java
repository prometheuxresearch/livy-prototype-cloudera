package com.example.livy.controller;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PostMapping;

import com.example.livy.job.MyJob;
import com.example.livy.jobExecutor.JobExecutor;
import com.example.livy.model.Program;

@Controller
public class LivyController {
	
	@GetMapping("/")
	public String index(Model model) {
		model.addAttribute("program", new Program());
		return "yourProgram";
	}
	
	@PostMapping("/evaluate")
	public String evaluate(@ModelAttribute Program p) throws Exception {
		String programContent = p.getProgramContent();
		String outputDir = p.getOutputDir();
		MyJob job = new MyJob(programContent, outputDir);
		JobExecutor.execute(job);
		return "nextProgram";
	}
	
	@GetMapping("/nextProgram")
	public String nextProgram(Model model) {
		model.addAttribute("program", new Program());
		return "yourProgram";
	}
	

}
