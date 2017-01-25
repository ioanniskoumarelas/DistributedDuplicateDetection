package de.hpi.is.idd.datasets;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import org.simmetrics.StringMetric;
import org.simmetrics.simplifiers.Simplifiers;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.simmetrics.builders.StringMetricBuilder.with;

public class FebrlUtility extends de.hpi.is.idd.interfaces.DatasetUtils implements scala.Serializable  {
    private final static String identifierJSON =
            "[[{\"attribute\":\"given_name\",\"similarityFunction\":\"JaroWinkler\",\"weight\":0.2}," +
            "{\"attribute\":\"surname\",\"similarityFunction\":\"Levenshtein\",\"weight\":0.4}, " +
            "{\"attribute\":\"soc_sec_id\",\"similarityFunction\":\"Levenshtein\",\"weight\":0.3}," +
            "{\"attribute\":\"age\",\"similarityFunction\":\"Equal\",\"weight\":0.1}]]";
    private static final long serialVersionUID = -8278150440957880041L;

    private final HashMap<String, Attribute> attributeMapping;
    private final List<List<String>> blockingKeys;
    public FebrlUtility() {
        datasetThreshold = 0.8;
        attributeMapping = new HashMap<>();
        blockingKeys = new ArrayList<>();
        Gson gson = new Gson();
        Type type = new TypeToken<LinkedList<LinkedList<Attribute>>>() {
        }.getType();
        LinkedList<LinkedList<Attribute>> identifiers = gson.fromJson(identifierJSON, type);
        identifiers.stream().forEach(identifier -> {
            List<String> blockingKey = new ArrayList<>();
            identifier.stream().forEach(attribute -> {
                        blockingKey.add(attribute.getAttribute());
                        attributeMapping.put(attribute.getAttribute(), attribute);
                    }
            );
            blockingKeys.add(blockingKey);
        });

    }

    /**
     * Given two records, return their similarity in the range of [0,1].
     *
     * @param record1
     * @param record2
     * @param parameters : You could pass your parameters in a key, value form.
     *
     * @return: The similarity in a double value of a range [0,1].
     */
    @Override
    public Double calculateSimilarity(final Map<String, Object> record1, final Map<String, Object> record2,
                                      final Map<String, String> parameters) {
        return blockingKeys.stream()
                .mapToDouble(attributeNames -> attributeNames.stream()
                        .mapToDouble(attributeName -> {
                            try {
                                Attribute attribute = attributeMapping.get(attributeName);
                                return attribute.getWeight() * SimilarityFunctions
                                        .getDistanceForStringMetric(
                                                record1.get(attributeName),
                                                record2.get(attributeName),
                                                attribute.getSimilarityFunction());
                            } catch (final ClassNotFoundException |
                                    IllegalAccessException |
                                    InstantiationException e) {
                                e.printStackTrace();
                                throw new RuntimeException();
                            }

                        }).sum()
                ).sum() / blockingKeys.size();
    }

    /**
     * @param values
     * @return A dictionary with key-value objects: e.g. <attribute1, value1> Each value can be of any
     *                      type, thus it is Object (and not String).
     */
    @Override
    public Map<String, Object> parseRecord(final Map<String, String> values) {
        return new HashMap<>(values);
    }

    @Override
    public Double compareAttributeValue(String attribute, Object value1, Object value2) {
        if(attributeMapping.containsKey(attribute)) {
            try {
                return SimilarityFunctions.getDistanceForStringMetric(value1, value2, attributeMapping.get(attribute)
                        .getSimilarityFunction());
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InstantiationException e) {
                e.printStackTrace();
            }
        }
        return 0.0;
    }

    public Boolean isMatch(Map<String, Double> similarities) {
        Double totalWeight = 0.0;
        Double sum = 0.0;
        for(Map.Entry<String, Double> similarityEntry : similarities.entrySet()) {
            String attribute = similarityEntry.getKey();
            if(!attributeMapping.containsKey(attribute)) return false;
            sum += attributeMapping.get(attribute).getWeight() * similarityEntry.getValue();
            totalWeight += attributeMapping.get(attribute).getWeight();
        }
        return sum/totalWeight >= this.getDatasetThreshold();
    }

    public List<List<String>> getBlockingKeys() {
        return blockingKeys;
    }

    public Object parseAttribute(final String attribute, final String value) {
        return value;
    }

    /**
     * This class represents a attribute of the data set with the corresponding similarity function
     */
    private class Attribute implements Serializable {

        private static final long serialVersionUID = -4016706663794117103L;
        final private String attribute;
        final private String similarityFunction;
        final private float weight;

        public Attribute(final String attribute, final String similarityFunction, final float weight) {
            this.attribute = attribute;
            this.similarityFunction = similarityFunction;
            this.weight = weight;
        }

        private String getAttribute() {
            return attribute;
        }

        private String getSimilarityFunction() {
            return similarityFunction;
        }

        public float getWeight() {
            return weight;
        }
    }

    private static class SimilarityFunctions {

        static private double getDistanceForStringMetric(final Object str1, final Object str2,
                                                         final String similarityFunction)
                throws ClassNotFoundException, IllegalAccessException, InstantiationException {
            if ((str1 == null) || str1.toString().isEmpty() || (str2 == null)|| str2.toString().isEmpty()) {
                return 0;
            }
            if (similarityFunction.equals("Equal"))
                return str1.equals(str2) ? 1 : 0;
            else {
                final StringMetric metric =
                        with((StringMetric) Class.forName("org.simmetrics.metrics." + similarityFunction).newInstance())
                                .simplify(Simplifiers.toLowerCase(Locale.ENGLISH))
                                .simplify(Simplifiers.replaceNonWord())
                                .build();
                return metric.compare(str1.toString(), str2.toString());
            }
        }
    }

	@Override
	public Double calculateAttributeSimilarity(Map<String, Double> similarities) {
		// TODO Auto-generated method stub
		return null;
	}
}
