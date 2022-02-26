package es.udc.fi.ri.mri_searcher.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;


public class ReadFile {

	public static List<String> readQuery(Path path) throws IOException{
		
		List<String> queries = new ArrayList<String>(); 
		
		try (InputStream stream = Files.newInputStream(path)) {
			try(BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));){
				String line = "";
				while ((line = reader.readLine()) != null) {
					String content = "";
					while ((line = reader.readLine()) != null) {	
						if(line.equals("/")) {
							queries.add(content);
							break;
						}
						content = content + line + " ";					
					}
				}					
			}
		}
		return queries;
	}
	
	public static List<List<Integer>> readRelevance(Path path) throws IOException{
		
		List<List<Integer>> relevants = new ArrayList<List<Integer>>(); 
		
		try (InputStream stream = Files.newInputStream(path)) {
			try(BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));){
				String line = "";
				
				while ((line = reader.readLine()) != null) {
					List<Integer> docs = new ArrayList<Integer>(); 
					
					while ((line = reader.readLine()) != null) {	
						if(line.equals("   /")) {
							relevants.add(docs);
							break;
						}
						String[] numbers = line.split("\\s+");
						for(String number: numbers) {
							if (StringUtils.isNumeric(number)) {
								docs.add(Integer.parseInt(number));
							}
						}
					}
				}					
			}
		}
		return relevants;
	}
	
}
