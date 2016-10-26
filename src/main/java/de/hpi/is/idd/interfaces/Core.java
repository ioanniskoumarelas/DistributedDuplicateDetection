package de.hpi.idd;

import java.util.List;
import java.util.Map;

public interface Core {

	/**
	 * 
	 * In this method, you should instantiate all the necessary index parts in order to be able to handle
	 * insertRecord, removeRecord and getDuplicates queries.
	 * 
	 * @param parameters: You could pass your parameters in a key, value form.
	 */
	public void buildIndex(Map<String, String> parameters);
		
	/**
	 * 
	 * Insert the record in the index.
	 * 
	 * If the duplicate IDs are available, return them in a List.
	 * 
	 * @param record: A generic, CSV-like type to provide the values of the record in a Key-Value style.
	 * @return: Similarly to the getDuplicates, return the record IDs in a List container. If the algorithm, however,
	 * 			is not able by its nature to also return the duplicates, return null.
	 */
	public List<String> insertRecord(Map<String, Object> record, Map<String, String> parameters);
	
	/**
	 * 
	 * Given a duplicate, return all the matching records you could find in your index WITHOUT ANY INSERTS.
	 * 
	 * If the record has an ID:
	 * 1) If the ID is NULL. You cannot use it. Just search for duplicates.
	 * 2) If the ID is not NULL.
	 *   2.1) If ID is in your DB/Index, you can use it to get the duplicates of the already inserted record.
	 *   2.2) If ID is not in your DB/Index, you cannot use it. Just search for duplicates. 
	 * 
	 * @param record: A generic, CSV-like type to provide the values of the record in a Key-Value style.
	 * @param parameters: Same as record.
	 * @return: The result record IDs in a List container.
	 */
	public List<String> getDuplicates(Map<String, Object> record, Map<String, String> parameters);
	
	/**
	 * 
	 * You should deallocate all the data structures.
	 * 
	 * @param parameters: You could pass your parameters in a key, value form.
	 * @return: true if successful, false otherwise
	 */
	public boolean destroyIndex(Map<String, String> parameters);
	
	/**
	 * Get the record with this record ID.
	 * @param recordID
	 * @return the record in a key-value format.
	 */
	public Map<String, Object> getRecord(String recordID);
}
