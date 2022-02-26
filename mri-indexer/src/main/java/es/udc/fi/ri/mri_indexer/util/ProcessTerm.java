package es.udc.fi.ri.mri_indexer.util;

public class ProcessTerm{
	public String name;
	public long tf;
	public long df;
	public Double tfidf;
	
	public ProcessTerm(String name, long tf,long df, Double tfidf) {
		this.name = name;
		this.tf=tf;
		this.df=df;
		this.tfidf=tfidf;
	}
}
