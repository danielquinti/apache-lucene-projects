package es.udc.fi.ri.mri_indexer.util;

public class SimTerm {
	public String name;
	public double similarity;
	
	public SimTerm(String name, double similarity) {
		this.name = name;
		this.similarity=similarity;
	}
}