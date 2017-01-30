package de.hpi.is.idd.datasets;

import info.debatty.java.stringsimilarity.JaroWinkler;
import info.debatty.java.stringsimilarity.NormalizedLevenshtein;
import org.simmetrics.StringDistance;
import org.simmetrics.metrics.HammingDistance;
import org.simmetrics.metrics.Levenshtein;
import org.simmetrics.metrics.MongeElkan;

import java.io.Serializable;
import java.util.*;

public class NCVotersSimpleUtility extends NCVotersIDDUtility implements Serializable {
	protected transient StringDistance hd = null;
	protected transient MongeElkan me = null;

	public NCVotersSimpleUtility() {
		datasetThreshold = 0.75;
		hd = HammingDistance.forString();
		me =  new MongeElkan(new Levenshtein());
	}

	public Double calculateSimilarity(Map<String, Object> record1, Map<String, Object> record2, Map<String, String> parameters) {
		Double specialCases = treatSpecialCases(record1, record2);
		if (specialCases >= 0) {
			return specialCases;
		}

        if (hd == null) {
            hd = HammingDistance.forString();
        }
        if (me == null) {
            me = new MongeElkan(new Levenshtein());
        }
        if (jw == null) {
            jw = new JaroWinkler();
        }
        if (nl == null) {
            nl = new NormalizedLevenshtein();
        }

		Double simFN = objectSimilarity(record1.get(FIRST_NAME), record2.get(FIRST_NAME));
		Double simMN = objectSimilarity(record1.get(MIDDLE_NAME), record2.get(MIDDLE_NAME));
		Double simLN = objectSimilarity(record1.get(LAST_NAME), record2.get(LAST_NAME));

		List<String> streetToks1 = new ArrayList<>(Arrays.asList(((String)record1.get(STREET)).split(" ")));
		List<String> streetToks2 = new ArrayList<>(Arrays.asList(((String)record2.get(STREET)).split(" ")));
		Double simSTR = 0.0;
		if (streetToks1.size() != 0 && streetToks2.size() != 0) {
			simSTR = (double) me.compare(streetToks1, streetToks2);
		}

		Double simHSNB = 0.0;
		if(record1.size() != record2.size()) {
			simHSNB = (double) hd.distance((String)record1.get(HOUSE_NUM), (String)record2.get(HOUSE_NUM)) / record1.size();
		}

		double sim = simFN * 0.15 + simMN * 0.1 + simLN * 0.25 + simSTR * 0.35 + simHSNB * 0.15;

		return sim;
	}
}
