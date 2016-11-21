package de.hpi.is.ddd.evaluation;

import de.hpi.is.ddd.evaluation.utils.UnionFind;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.tuple.Pair;


import java.io.File;
import java.io.FileReader;
import java.util.*;

/**
 *
 * Generates an Evaluation given the gold standard and the current algorithms performance.
 *
 * Evaluator.java
 */
public class Evaluator {
    private final static CSVFormat FORMAT = CSVFormat.TDF.withFirstRecordAsHeader();

    /* Gold Standard file path */
    private File goldStandard;

    /* Gold Standard */
    private UnionFind<String> ufGS;
    private Set<Pair<String, String>> pairsGS;

//    XXX: Keep ufA OR pairsA in the Evaluator?
//    /* Algorithm */
//    private UnionFind<String> ufA;
//    private Set<Pair<String, String>> pairsA;

    private long totalComparisons;

    private long startTime;

    public Evaluator(File goldStandard) {
        this.totalComparisons = 0;
        this.goldStandard = goldStandard;
        pairsGS = new HashSet<Pair<String, String>>();
        try (CSVParser parser = new CSVParser(new FileReader(goldStandard), FORMAT)) {
            Iterator<CSVRecord> itRec = parser.iterator();
            while (itRec.hasNext()) {
                CSVRecord rec = itRec.next();
                pairsGS.add(Pair.of(rec.get("id1"), rec.get("id2")));
            }
            ufGS = convertPairsToUnionFind(pairsGS);
            startTime = System.currentTimeMillis();
        } catch (java.io.IOException e) {
            System.out.println("There was a problem while reading the gold standard. Aborting...");
            e.printStackTrace();
            System.exit(1);
        }
    }

    public Evaluation evaluate(Set<Pair<String, String>> pairsA) {
        return evaluate(convertPairsToUnionFind(pairsA));
    }

    /**
     * Calculates the number of correct or wrong pairs, using the transitivity for both the gold standard and
     *   the algorithm's pairs.
     *
     *   @param ufA: The UnionFind of the algorithm's result.
     */
    public Evaluation evaluate(UnionFind<String> ufA) {
        int tp = 0, fp = 0, fn = 0;

        /* What the result found
        *  Results --> GoldStandard */
        for (Set<String> component : ufA) {
            List<String> elements = new ArrayList<>(component);
            for (int i = 0; i < elements.size(); i++) {
                for (int j = i + 1; j < elements.size(); j++) {
                    if (ufGS.connected(elements.get(i), elements.get(j))) {
                        ++tp;
                    } else {
                        ++fp;
                    }
                }
            }
        }

        /* What the result should have found
        *  GroundTruth --> Results */
        /* For the following, we are going to need to remove the elements that we haven't met, from the components. */
        int pairsGS = 0;  // Pairs in Gold Standard
        int entriesGS = 0; // Entries in Gold Standard
        for (Set<String> component : ufGS) {
            List<String> elements = new ArrayList<>(component);
            entriesGS += elements.size();
            for (int i = 0; i < elements.size(); i++) {
                for (int j = i + 1; j < elements.size(); j++) {
                    ++pairsGS;
                    if (!ufA.connected(elements.get(i), elements.get(j))) {
                        ++fn;
                    }
                }
            }
        }

        Evaluation evl = new Evaluation();
        evl.setTp(tp);
        int allPossiblePairs = (int) (entriesGS * (entriesGS - 1) / 2.0);
        int tn = allPossiblePairs - pairsGS - fp;
        evl.setTn(tn);
        evl.setFp(fp);
        evl.setFn(fn);

        evl.setExecutionTime(System.currentTimeMillis() - startTime);

        evl.setAvailableProcessors(Runtime.getRuntime().availableProcessors());

        evl.setMaxMemory((Runtime.getRuntime().maxMemory() / 1024) / 1024);
        evl.setTotalMemory((Runtime.getRuntime().totalMemory() / 1024) / 1024);
        evl.setFreeMemory((Runtime.getRuntime().freeMemory() / 1024) / 1024);
        evl.setUsedMemory(evl.getTotalMemory() - evl.getFreeMemory());

        evl.setTotalComparisons(totalComparisons);


        return evl;
    }

    public static UnionFind<String> convertPairsToUnionFind(Set<Pair<String, String>> pairs) {
        UnionFind<String> uf = new UnionFind<>();

        for (Pair<String, String> p: pairs) {
            uf.union(p.getKey(), p.getValue());
        }

        return uf;
    }

    public void setTotalComparisons(long totalComparisons) {
        this.totalComparisons = totalComparisons;
    }
}
