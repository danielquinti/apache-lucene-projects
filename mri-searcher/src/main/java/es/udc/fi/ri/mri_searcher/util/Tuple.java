package es.udc.fi.ri.mri_searcher.util;

public class Tuple {
	private Float hyperparameter;
	private Double metrica;
	
	public Tuple(Float x, Double y) {
		this.hyperparameter=x;
		this.metrica=y;
	}
	public Float getHyperparameter() {
		return this.hyperparameter;
	}
	
	public Double getMetrica() {
		return this.metrica;
	}
	
	public void setMetrica(Double x) {
		this.metrica=x;
	}


}
