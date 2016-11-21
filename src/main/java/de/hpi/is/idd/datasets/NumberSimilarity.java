package de.hpi.is.idd.datasets;

import info.debatty.java.stringsimilarity.interfaces.NormalizedStringDistance;

public class NumberSimilarity implements NormalizedStringDistance {

    private static final long serialVersionUID = 1L;

	public double distance(String i1, String i2) {
		int length = Math.min(i1.length(), i2.length());
		int matching_digits = 0;
		for (int i = 0; i < length; i++) {
			if (i1.charAt(i) == i2.charAt(i)) matching_digits++;
		}
		return (double)matching_digits / length;
	}
}
