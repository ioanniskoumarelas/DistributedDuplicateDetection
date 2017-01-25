package de.hpi.is.idd.datasets;

import info.debatty.java.stringsimilarity.JaroWinkler;
import info.debatty.java.stringsimilarity.NormalizedLevenshtein;

import org.apache.commons.codec.language.DoubleMetaphone;
import org.simmetrics.StringDistance;
import org.simmetrics.StringMetric;
import org.simmetrics.metrics.HammingDistance;
import org.simmetrics.metrics.Levenshtein;
import org.simmetrics.metrics.MongeElkan;

import java.io.Serializable;
import java.util.*;

public class NCVotersUtility extends de.hpi.is.idd.interfaces.DatasetUtils implements Serializable {

	static final String ID = "id";
	static final String FIRST_NAME = "first_name";
	static final String MIDDLE_NAME = "midl_name";
	static final String LAST_NAME = "last_name";
	static final String HOUSE_NUM = "house_num";
	static final String STREET = "street_name";
	static final String ZIP_CODE = "zip_code";
	static final String AGE = "age";
	static final String SEX = "sex_code";
	static final String BIRTH_PLACE = "birth_place";
	static final String STATUS = "voter_status_desc";
	static final String STATUS_REASON = "voter_status_reason_desc";
	static final String RACE = "race_desc";
	static final String PARTY = "party_cd";
	static final String COUNTY = "county_id";
	private static final long serialVersionUID = -7726140071035852774L;

	JaroWinkler jw = new JaroWinkler();
	NormalizedLevenshtein nl = new NormalizedLevenshtein();

	StringDistance hd = HammingDistance.forString();
	MongeElkan me = new MongeElkan(new Levenshtein());
	
	// sum of distances for all attributes
	double distances;
	
	// attributes that are considered for similarity
	int attribute_count;
	
	public NCVotersUtility() {
		datasetThreshold = 0.75;
	}
	
	private Double treatSpecialCases(Map<String, Object> record1, Map<String, Object> record2) {
		// same record?
		if (record1.get(ID) == record2.get(ID))
			return 1.0;
		
		// return 0 if one is male and the other female
		if ((record1.get(SEX).equals("M") && record2.get(SEX).equals("F"))
				|| (record1.get(SEX).equals("F") && record2.get(SEX).equals("M")))
			return 0.0;
		
		// return 0 if birth_place, race or sex  don't match
		String r1 = (String) record1.get(RACE);
		String r2 = (String) record2.get(RACE);
		if ((!r1.isEmpty()) && (!r2.isEmpty()) && (!r1.equals(r2)) && (!r1.equals("UNDESIGNATED")) 
				&& (!r2.equals("UNDESIGNATED")) && (!r1.equals("OTHER")) && (!r2.equals("OTHER")))
			return 0.0;

		// only one record can be active at a time
		if (record1.get(STATUS).equals("ACTIVE") && record2.get(STATUS).equals("ACTIVE"))
			return 0.0;
		
		// only one record can be removed because of death
		if (record1.get(STATUS_REASON).equals("DECEASED") && record2.get(STATUS_REASON).equals("DECEASED"))
			return 0.0;
		
		// deceased person cannot have an active record
		if ((record1.get(STATUS).equals("ACTIVE") && record2.get(STATUS_REASON).equals("DECEASED"))
				|| (record2.get(STATUS).equals("ACTIVE") && record1.get(STATUS_REASON).equals("DECEASED"))) 
			return 0.0;
		
		// age has to be the same
		if (record1.get(AGE) != record2.get(AGE))
			return 0.0;
		
		return -1.0;
	}
	
	private double objectSimilarity(Object o1, Object o2) {
		return objectSimilarity(o1, o2, "");
	}
	
	private double objectSimilarity(Object o1, Object o2, Object def) {
		if (o1.equals(def) && o2.equals(def))
			return 1.0;
		else if (o1.equals(def) || o2.equals(def)) {
			attribute_count--;
			return 0.0;
		} else {
			if (o1 instanceof Integer)
				return 1 - hammingDistance(o1.toString(), o2.toString());
			else
				return 1 - nl.distance(o1.toString(), o2.toString());
		}
	}

	public Double calculateSimilarityNaive(Map<String, Object> record1, Map<String, Object> record2, Map<String, String> parameters) {
        Double specialCases = treatSpecialCases(record1, record2);
        if (specialCases >= 0) {
            return specialCases;
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

    public Double calculateSimilarityIDD(Map<String, Object> record1, Map<String, Object> record2, Map<String, String> parameters) {
        Double specialCases = treatSpecialCases(record1, record2);
        if (specialCases >= 0)
            return specialCases;

        distances = 0;
        attribute_count = 5;

        distances += objectSimilarity(record1.get(BIRTH_PLACE), record2.get(BIRTH_PLACE));
        distances += (record1.get(PARTY).equals(record2.get(PARTY)) ? 1.0 : 0.0);

        // first name rarely changes by much, so large differences influence negatively
        distances += (1 - jw.distance((String) record1.get(FIRST_NAME), (String) record2.get(FIRST_NAME))) * 2 - 1;

        // when marrying, old last name often moves to middle name
        // if this happens, ignore middle and last name, and only consider the rest
        if (record1.get(MIDDLE_NAME).equals(record2.get(LAST_NAME)) || record2.get(MIDDLE_NAME).equals(record1.get(LAST_NAME)))
            attribute_count -= 2;
        else {
            // at least two of the three attributes have to have the same DoubleMetaphone-Encoding
            int dms = 0;
            DoubleMetaphone dm = new DoubleMetaphone();
            if (dm.isDoubleMetaphoneEqual((String) record1.get(FIRST_NAME), (String) record2.get(FIRST_NAME))) dms++;
            if (dm.isDoubleMetaphoneEqual((String) record1.get(MIDDLE_NAME), (String) record2.get(MIDDLE_NAME))) dms++;
            if (dm.isDoubleMetaphoneEqual((String) record1.get(LAST_NAME), (String) record2.get(LAST_NAME))) dms++;
            if (dms < 2)
                return 0.0;

            distances += objectSimilarity(record1.get(MIDDLE_NAME), record2.get(MIDDLE_NAME));
            distances += objectSimilarity(record1.get(LAST_NAME), record2.get(LAST_NAME));
        }

        // calculate difference of addresses
        double address_distance = 0.0, street_distance = 0.0;
        int address_attributes = 3, old_attribute_count = attribute_count;
        street_distance += objectSimilarity(record1.get(STREET), record2.get(STREET), "UNKNOWN");
        address_distance += street_distance;
        if (old_attribute_count != attribute_count) street_distance = 1.0;
        address_distance += objectSimilarity(record1.get(ZIP_CODE), record2.get(ZIP_CODE), 0) * street_distance;
        address_distance += objectSimilarity(record1.get(HOUSE_NUM), record2.get(HOUSE_NUM), 0) * street_distance;
        address_attributes += old_attribute_count - attribute_count;
        attribute_count = old_attribute_count;

        String s1, s2, mc, mw;
        Integer c1, c2;
        s1 = (String) record1.get(STATUS_REASON);
        s2 = (String) record2.get(STATUS_REASON);
        c1 = (Integer) record1.get(COUNTY);
        c2 = (Integer) record2.get(COUNTY);
        mc = "MOVED FROM COUNTY";
        mw = "MOVED WITHIN STATE";

        // if moved from county, and counties are different, or moved within state
        // consider address information less heavily
        if (((s1.equals(mc) || s2.equals(mc)) && (!c1.equals(c2))) || (s1.equals(mw) || s2.equals(mw)))	{
            distances += (address_distance / address_attributes);
            attribute_count += 1;
        } else {
            distances += address_distance;
            attribute_count += address_attributes;
        }

        return Double.max(distances / attribute_count, 0.0);
    }
	
	public Double calculateSimilarity(Map<String, Object> record1, Map<String, Object> record2, Map<String, String> parameters) {	
		return calculateSimilarityIDD(record1, record2, parameters);
//		return calculateSimilarityNaive(record1, record2, parameters);
	}
	
	public Map<String, Object> parseRecord(Map<String, String> values) {
		HashMap<String, Object> result = new HashMap<>();
		List<String> intColumns = Arrays.asList("house_num", "zip_code", "age", "county_id");
		for (String key: values.keySet()) {
			String value = values.get(key);
			if ((!value.isEmpty()) && (intColumns.contains(key))) {
				Integer intValue;
				try {
					intValue = Integer.parseInt(value);
				} catch (NumberFormatException e) {
					intValue = 0;
				}
				result.put(key, intValue);
			} else
				result.put(key, value);	
		}
		return result;
	}

	@Override
	public Double compareAttributeValue(String attribute, Object value1, Object value2) {
		if (value1 instanceof String)
			return jw.distance((String) value1, (String) value2);
		else if (value1 instanceof Integer)
			return hammingDistance(((Integer) value1).toString(), ((Integer) value2).toString());
		else
			return jw.distance(value1.toString(), value2.toString());
	}

	public double hammingDistance(String i1, String i2) {
		int length = Math.min(i1.length(), i2.length());
		int matching_digits = 0;
		for (int i = 0; i < length; i++) {
			if (i1.charAt(i) == i2.charAt(i)) matching_digits++;
		}
		return (double)matching_digits / length;
	}

	public Boolean isMatch(Map<String, Double> similarities) {
		double similarity = 0.0;
		int attributeCount = 0;
		for (String key: similarities.keySet()) {
			attributeCount++;
			if (key.equals(FIRST_NAME)) {
				similarity += 2 * similarities.get(key);
				attributeCount++;
			}
			// age is always identical for duplicate records in the dataset
			else if (key.equals(AGE)) {
				if (similarities.get(key) != 1.0)
					return false;
			}
			else
				similarity += similarities.get(key);
		}
		return (similarity / attributeCount) >= datasetThreshold;
	}

	@Override
	public Double calculateAttributeSimilarity(Map<String, Double> similarities) {
		// TODO Auto-generated method stub
		return null;
	};

}
