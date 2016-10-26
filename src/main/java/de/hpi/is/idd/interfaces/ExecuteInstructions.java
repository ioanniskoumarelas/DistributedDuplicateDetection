package de.hpi.idd;

import java.io.File;
import java.util.Map;

public interface ExecuteInstructions {

	
	/**
	 * Execute any of the instructions:
	 * 
	 * buildIndex
	 * insert
	 * (remove)
	 * getDuplicates
	 * destroyIndex
	 * 
	 * @param instruction: Any of the above instructions.
	 * @param record: The record as returned by DatasetUtils.parseRecord method.
	 */
	public void executeInstruction(String instruction, Map<String, Object> record);
	
	
	/**
	 * Reading each instruction line by line, executing 'executeInstruction' method,
	 * outputting the results to the fileOut.
	 * 
	 * @param fileIn: input instructions file
	 * @param fileOut: output results file
	 * @param parameters
	 */
	public void executeFile(File fileIn, File fileOut, Map<String, String> parameters);
	
}
