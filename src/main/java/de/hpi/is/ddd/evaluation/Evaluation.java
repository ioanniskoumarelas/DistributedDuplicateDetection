package de.hpi.is.ddd.evaluation;

/**
 * The final result that can describe how did the algorithm perform overall.
 *
 * Evaluation.java
 */
public class Evaluation {

    int availableNodes = Integer.MIN_VALUE;

    /* Change to <node id, # processors> */
    int availableProcessors = Integer.MIN_VALUE;

    long executionTime = Long.MIN_VALUE;

    long totalMemory = Long.MIN_VALUE;
    long maxMemory = Long.MIN_VALUE;
    long usedMemory = Long.MIN_VALUE;
    long freeMemory = Long.MIN_VALUE;

    int tp;
    int tn;
    int fp;
    int fn;

    long totalComparisons = 0L;

    @Override
    public String toString() {
        return  " tp: " + tp +
                " tn: " + tn +
                " fp: " + fp +
                " fn: " + fn +
                " precision: " + calculatePrecision() +
                " recall: " + calculateRecall() +
                " fmeasure: " + calculateFMeasure() +
                " executionTime (ms): " + executionTime +
                " totalComparisons: " + totalComparisons +
                " availableProcessors: " + availableProcessors +
                " totalMemory: " + totalMemory +
                " maxMemory: " + maxMemory +
                " usedMemory: " + usedMemory +
                " freeMemory: " + freeMemory;
    }

    public double calculatePrecision() {
        return tp / ((double) tp + fp);
    }

    public double calculateRecall() {
        return tp / ((double) tp + fn);
    }

    public double calculateFMeasure() {
        return calculateFMeasure(1.0, calculatePrecision(), calculateRecall());
    }

    public double calculateFMeasure(double beta, double precision, double recall) {
        return (1 + Math.pow(beta, 2.0)) * (precision * recall) / (Math.pow(beta, 2.0) * precision + recall);
    }

    public int getAvailableProcessors() {
        return availableProcessors;
    }

    public void setAvailableProcessors(int availableProcessors) {
        this.availableProcessors = availableProcessors;
    }

    public long getExecutionTime() {
        return executionTime;
    }

    public void setExecutionTime(long executionTime) {
        this.executionTime = executionTime;
    }

    public long getTotalMemory() {
        return totalMemory;
    }

    public void setTotalMemory(long totalMemory) {
        this.totalMemory = totalMemory;
    }

    public long getMaxMemory() {
        return maxMemory;
    }

    public void setMaxMemory(long maxMemory) {
        this.maxMemory = maxMemory;
    }

    public long getUsedMemory() {
        return usedMemory;
    }

    public void setUsedMemory(long usedMemory) {
        this.usedMemory = usedMemory;
    }

    public long getFreeMemory() {
        return freeMemory;
    }

    public void setFreeMemory(long freeMemory) {
        this.freeMemory = freeMemory;
    }

    public int getTp() {
        return tp;
    }

    public void setTp(int tp) {
        this.tp = tp;
    }

    public int getTn() {
        return tn;
    }

    public void setTn(int tn) {
        this.tn = tn;
    }

    public int getFp() {
        return fp;
    }

    public void setFp(int fp) {
        this.fp = fp;
    }

    public int getFn() {
        return fn;
    }

    public void setFn(int fn) {
        this.fn = fn;
    }

    public int getAvailableNodes() {
        return availableNodes;
    }

    public void setAvailableNodes(int availableNodes) {
        this.availableNodes = availableNodes;
    }

    public long getTotalComparisons() {
        return totalComparisons;
    }

    public void setTotalComparisons(long totalComparisons) {
        this.totalComparisons = totalComparisons;
    }
}
