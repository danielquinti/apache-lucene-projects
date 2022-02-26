package es.udc.fi.ri.mri_indexer.util;

import org.apache.commons.math3.linear.RealVector;

public class CosineSimilarity {
	private RealVector v1;
	private RealVector v2;

	public CosineSimilarity(RealVector v1, RealVector v2) {
		this.v1 = v1;
		this.v2 = v2;
	}

	public double calculate() {
		return (v1.dotProduct(v2)) / (v1.getNorm() * v2.getNorm());
	}
}
