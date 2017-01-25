package de.hpi.is.idd.datasets;

import com.opencsv.CSVWriter;

import org.simmetrics.StringMetric;
import org.simmetrics.metrics.StringMetrics;
import scala.Serializable;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by axel on 21.05.16.
 */
public class CoraUtility extends de.hpi.is.idd.interfaces.DatasetUtils implements Serializable {

    private static final long serialVersionUID = -3805141621258463961L;
    public static StringMetric levenshtein = StringMetrics.levenshtein();
    public static StringMetric jaroWinkler = StringMetrics.jaroWinkler();
    public static StringMetric longestCommonSubsequence = StringMetrics.longestCommonSubsequence();
    public static StringMetric mongeElkan = StringMetrics.mongeElkan();
    public static StringMetric jaccard = StringMetrics.generalizedJaccard();
    public double calls_to_sim_functions=0;

    protected double datasetThreshold = 0.8;

    public double get_calls_to_sim_functions(){
        return this.calls_to_sim_functions;
    }

    public double getDatasetThreshold() {
        return datasetThreshold;
    }

    public void setDatasetThreshold(double datasetThreshold) {
        this.datasetThreshold = datasetThreshold;
    }

    /**
     *
     * Given two records, return their similarity in the range of [0,1].
     *
     * @param record1
     * @param record2
     * @param parameters: You could pass your parameters in a key, value form.
     * @return: The similarity in a double value of a range [0,1].
     */
    public Double calculateSimilarity(Map<String, Object> record1, Map<String, Object> record2, Map<String, String> parameters){
        Double sim = 0.0;
        calls_to_sim_functions += 1;
        try {
            Float sim1 = mongeElkan.compare(record1.get("Authors").toString(), record2.get("Authors").toString());
            Float sim2 = levenshtein.compare(record1.get("title").toString(), record2.get("title").toString());
            Float sim3 = longestCommonSubsequence.compare(record1.get("pages").toString(), record2.get("pages").toString());
            this.calls_to_sim_functions += 2;
            sim = (double) (2 * sim1 * sim2) / (sim1 + sim2);
            if (sim3<0.5){
                sim -= 0.1;
            }
            if ((record1.get("book_or_tech").toString().equals("tech") || record1.get("book_or_tech").toString().equals("both") )
                    && (record2.get("book_or_tech").toString().equals("book") || record2.get("book_or_tech").toString().equals("both"))) {
                sim -= 1;
            }else if ((record1.get("book_or_tech").toString().equals("book") || record1.get("book_or_tech").toString().equals("both") )
                    && (record2.get("book_or_tech").toString().equals("tech") || record2.get("book_or_tech").toString().equals("both"))) {
                sim -= 1;
            }
            return Math.min(1, Math.max(sim,0));
        }catch(Exception e){
            e.printStackTrace();
            return sim;
        }
    }

    /**
     *
     * @param values in a Key-Value <String, String> format.
     * 			For instance: <'id', '1'>, <'attribute1', 'value1'>, <'attribute2', 'value2'>, ..., <'attributeN', valueN'>
     * @return A dictionary with key-value objects: e.g. <attribute1, value1>
     * 			Each value can be of any type, thus it is Object (and not String).
     */
    public Map<String, Object> parseRecord(Map<String, String> values){
        Map<String, Object> asd = new HashMap<>();
        asd.putAll(values);
        return asd;
    }


    @Override
    public Double compareAttributeValue(String attribute, Object value1, Object value2) {
        calls_to_sim_functions += 0.25;
        switch (attribute){
            case "Authors":
                return (double) (mongeElkan.compare(value1.toString(), value2.toString()));
            case "title":{
                return (double) (levenshtein.compare(value1.toString(), value2.toString()));
            }
            case "pages":
                double sim = longestCommonSubsequence.compare(value1.toString(), value2.toString());
                if (sim < 0.5) return -0.1;
                else return 0.0;
            case "book_or_tech":
                if ((value1.toString().equals("tech") || value1.toString().equals("both") )
                        && (value2.toString().equals("book") || value2.toString().equals("both"))) {
                    return -1.0;
                }
                else if ((value1.toString().equals("book") || value1.toString().equals("both") )
                        && (value2.toString().equals("tech") || value2.toString().equals("both"))) {
                    return -1.0;
                }
                else return 0.0;
            default:
                return 0.0;
        }

    }

    /**
     * Works now. Use it.
     */
    @Override
    public Double calculateAttributeSimilarity(Map<String, Double> similarities) {
        try {
            Double sim = 0.0;
            Double sim1 = similarities.get("Authors");
            Double sim2 = similarities.get("title");
            Double sim3 = similarities.get("pages");
            Double sim4 = similarities.get("book_or_tech");
            sim = (double) (2 * sim1 * sim2) / (sim1 + sim2);
            sim += sim3;
            sim += sim4;
            return Math.min(1, Math.max(sim,0));
        } catch (NullPointerException e) {
            e.printStackTrace();
            System.out.println(similarities);
            return 0.0;
        }
    }

//    @Override
//    public Double calculateAttributeSimilarityEx(Map<String, Double> precalcluatedSims, Map<String, Object> record1, Map<String, Object> record2){
//        Double sim = 0.0;
//        Double sim1 = precalcluatedSims.get("Authors");
//        if (sim1 == null){
//            sim1 = compareAttributeValue("Authors", record1.get("Authors"), record2.get("Authors"));
//        }
//        Double sim2 = precalcluatedSims.get("title");
//        if (sim2 == null){
//            sim2 = compareAttributeValue("title", record1.get("title"), record2.get("title"));
//        }
//        //Double sim2 = precalcluatedSims.getOrDefault("title", compareAttributeValue("title", record1.get("title"), record2.get("title")));
//        Float sim3 = longestCommonSubsequence.compare(record1.get("pages").toString(), record2.get("pages").toString());
//        sim = (double) (2 * sim1 * sim2) / (sim1 + sim2);
//        if (sim3<0.5){
//            sim -= 0.1;
//        }
//        if (record1.get("tech").toString().length() > 1 && record2.get("bookTitle").toString().length() > 1) {
//            sim -= 1;
//        }
//        if (record2.get("tech").toString().length() > 1 && record1.get("bookTitle").toString().length() > 1) {
//            sim -= 1;
//        }
//        return Math.min(1, Math.max(sim,0));
//
//    }

    /**
     *
     * @param filePath: file to cora.csv
     * @return list of records of type Map<String, Object> found in the csv file
     * @throws IOException
     */
    public static ArrayList<Map<String, String>> processCSV(String filePath) throws IOException {
        ArrayList<Map<String, String>> records = new ArrayList<>();
        String stringRead ="";
        try
        {
            int count = 1;
            FileReader fr = new FileReader(filePath);
            BufferedReader br = new BufferedReader(fr);
            stringRead = br.readLine();
            ArrayList<String> fields = new ArrayList<>();
            String[] splittetFields = stringRead.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
            for (String field : splittetFields){
                fields.add(field.replace("\"", "").trim());
            }

            stringRead = br.readLine();

            while( stringRead != null )
            {
                String[] splittetString = stringRead.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
                Map<String, String> record = new HashMap<>();
                for (int i=0; i<splittetString.length && i<splittetFields.length; i++){
                    record.put(fields.get(i), splittetString[i].replace("\"", "").trim());
                }
                records.add(record);
                stringRead = br.readLine();
                count ++;
            }
            br.close( );
        }
        catch (Exception e){
            System.out.println(stringRead);
            e.printStackTrace();
        }
        return records;

    }

    public static void writeCleanedCSV(String fileInPath) throws IOException {
        List<String> attrs_to_keep = Arrays.asList("id", "Authors", "title", "pages");
        ArrayList<Map<String, String>> records = processCSV(fileInPath);
        FileWriter fw = new FileWriter("/media/axel/uni/idd/repo/irl/data/cora_v3.csv");
        CSVWriter writer = new CSVWriter(fw);
        List<String[]> lines = new ArrayList<>();
        List<String> attrs = new LinkedList<>();
        attrs.addAll(attrs_to_keep);
        attrs.add("book_or_tech");
        lines.add(attrs.toArray(new String[attrs.size()]));
        writer.writeAll(lines);
        for(int i = 0; i<records.size(); i++){
            Map<String, String> record= records.get(i);
            List<String> newRecord = new LinkedList<>();
            attrs_to_keep.forEach(attr -> newRecord.add(record.get(attr)));
            if (record.get("tech").toString().length() > 1 && record.get("bookTitle").toString().length() < 1) {
                newRecord.add("tech");
            }else if (record.get("tech").toString().length() < 1 && record.get("bookTitle").toString().length() > 1){
                newRecord.add("book");
            }else if(record.get("tech").toString().length() < 1 && record.get("bookTitle").toString().length() < 1){
                newRecord.add("neither");
            }else{
                newRecord.add("both");
            }
            writer.writeNext(newRecord.toArray(new String[newRecord.size()]));
        }
        writer.flush();
        //

    }



}
