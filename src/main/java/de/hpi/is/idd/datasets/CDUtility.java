package de.hpi.is.idd.datasets;

import org.apache.commons.lang3.StringUtils;
import scala.Serializable;

import java.text.Normalizer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

public class CDUtility extends de.hpi.is.idd.interfaces.DatasetUtils implements Serializable {

	private static final long serialVersionUID = -7666942644356996786L;

	static class CDRecord {

		private String artist;
		private String category;
		private String cdExtra;
		private String genre;
		private String id;
		private String title;
		private List<String> tracks;
		private Short year;

		public CDRecord() {
		}

		public CDRecord(String id, String artist, String title, String category, String genre, String cdExtra,
				short year, List<String> tracks) {
			this.id = id;
			this.artist = artist;
			this.title = title;
			this.category = category;
			this.genre = genre;
			this.cdExtra = cdExtra;
			this.year = year;
			this.tracks = tracks;
		}

		public String getArtist() {
			return artist;
		}

		public String getCategory() {
			return category;
		}

		public String getCdExtra() {
			return cdExtra;
		}

		public String getGenre() {
			return genre;
		}

		public String getId() {
			return id;
		}

		public String getTitle() {
			return title;
		}

		public List<String> getTracks() {
			return tracks;
		}

		public Short getYear() {
			return year;
		}

		public void setArtist(String artist) {
			this.artist = artist;
		}

		public void setCategory(String category) {
			this.category = category;
		}

		public void setCdExtra(String cdExtra) {
			this.cdExtra = cdExtra;
		}

		public void setGenre(String genre) {
			this.genre = genre;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setTitle(String title) {
			this.title = title;
		}

		public void setTracks(List<String> tracks) {
			this.tracks = tracks;
		}

		public void setYear(short year) {
			this.year = year;
		}

		@Override
		public String toString() {
			return id;
		}
	}

	private static class CDRecordParser {

		@SuppressWarnings("unchecked")
		public static CDRecord parse(Map<String, Object> record) {
			CDRecord cd = new CDRecord();
			cd.setId((String) record.get(ID));
			cd.setArtist((String) record.get(ARTIST_NAME));
			cd.setTitle((String) record.get(TITLE_NAME));
			cd.setCategory((String) record.get(CATEGORY_NAME));
			Object year = record.get(YEAR_NAME);
			if (year != null) {
				cd.setYear((Short) year);
			}
			cd.setGenre((String) record.get(GENRE_NAME));
			cd.setCdExtra((String) record.get(CDEXTRA_NAME));
			cd.setTracks((List<String>) record.get(TRACKS_NAME));
			return cd;
		}
	}

	public enum Attribute {
		ARTIST(ARTIST_NAME, 5),
		CATEGORY(CATEGORY_NAME),
		CDEXTRA(CDEXTRA_NAME, 0),
		GENRE(GENRE_NAME),
		TITLE(TITLE_NAME, 4),
		TRACKS(TRACKS_NAME, 3),
		YEAR(YEAR_NAME);

		private static final double TOTAL_WEIGHT = Arrays.stream(Attribute.values()).mapToDouble(Attribute::weight)
				.sum();
		private final double weight;
		public final String name;

		Attribute(String name) {
			this(name, 1);
		}

		Attribute(String name, double weight) {
			this.name = name;
			this.weight = weight;
		}

		public double weight() {
			return weight;
		}

		public String getName() {
			return name;
		}

		public static Attribute getForName(String attribute) {
			switch (attribute) {
			case ARTIST_NAME:
				return ARTIST;
			case TITLE_NAME:
				return TITLE;
			case CATEGORY_NAME:
				return CATEGORY;
			case GENRE_NAME:
				return GENRE;
			case YEAR_NAME:
				return YEAR;
			case CDEXTRA_NAME:
				return CDEXTRA;
			case TRACKS_NAME:
				return TRACKS;
			default:
				throw new IllegalArgumentException("Unknown attribute: " + attribute);
			}
		}
	}

	private static final String ARTIST_NAME = "artist";
	private static final String CATEGORY_NAME = "category";
	private static final String YEAR_NAME = "year";
	private static final String TITLE_NAME = "dtitle";
	private static final String CDEXTRA_NAME = "cdextra";
	private static final String TRACKS_NAME = "tracks";
	private static final String GENRE_NAME = "genre";
	private static final String ID = "id";
	private static final String SEPERATOR = "\\|";
	private static final double THRESHOLD = 0.7;

	private static int getNthDigit(int number, int n) {
		return (int) (Math.abs(number) / Math.pow(10, n) % 10);
	}

	private static int getNumberOfDigits(int number) {
		return (int) (Math.log10(Math.abs(number)) + 1);
	}

	public Map<String, Double> getSimilarityOfRecords(CDRecord firstRecord, CDRecord secondRecord) {
		Map<String, Double> similarityMap = new HashMap<>();
		similarityMap.put(ARTIST_NAME,
				compareAttributeValue(ARTIST_NAME, firstRecord.getArtist(), secondRecord.getArtist()));
		similarityMap.put(TITLE_NAME,
				compareAttributeValue(TITLE_NAME, firstRecord.getTitle(), secondRecord.getTitle()));
		similarityMap.put(CATEGORY_NAME,
				compareAttributeValue(CATEGORY_NAME, firstRecord.getCategory(), secondRecord.getCategory()));
		similarityMap.put(GENRE_NAME,
				compareAttributeValue(GENRE_NAME, firstRecord.getGenre(), secondRecord.getGenre()));
		similarityMap.put(YEAR_NAME, compareAttributeValue(YEAR_NAME, firstRecord.getYear(), secondRecord.getYear()));
		similarityMap.put(CDEXTRA_NAME,
				compareAttributeValue(CDEXTRA_NAME, firstRecord.getCdExtra(), secondRecord.getCdExtra()));
		similarityMap.put(TRACKS_NAME,
				compareAttributeValue(TRACKS_NAME, firstRecord.getTracks(), secondRecord.getTracks()));
		return similarityMap;
	}

	private static double getSimilarityOfTracks(List<String> firstTracklist, List<String> secondTracklist) {
		HashSet<String> set = new HashSet<>(firstTracklist);
		set.retainAll(secondTracklist);
		int shared = set.size();
		Set<String> mergedTrackset = new HashSet<>();
		mergedTrackset.addAll(firstTracklist);
		mergedTrackset.addAll(secondTracklist);
		if (mergedTrackset.isEmpty()) {
			return 1.0;
		}
		return (double) shared / mergedTrackset.size();
	}

	private static double levenshteinDistance(String a, String b) {
		if (a.isEmpty() || b.isEmpty()) {
			return THRESHOLD;
		}
		return 1.0 - (double) StringUtils.getLevenshteinDistance(a.toLowerCase(), b.toLowerCase())
				/ Math.max(a.length(), b.length());
	}

	private static String normalize(String s) {
		s = s.toLowerCase();
		s = Normalizer.normalize(s, Normalizer.Form.NFD);
		s = s.replaceAll("\\p{M}", "");
		s = s.trim();
		return s;
	}

	public double similarity(CDRecord firstRecord, CDRecord secondRecord) {
		Map<String, Double> similarityMap = getSimilarityOfRecords(firstRecord, secondRecord);
		return calculateAttributeSimilarity(similarityMap);
	}

	private static String trimNumbers(String s) {
		s = s.replaceAll("^\\d+\\s+", "");
		return s;
	}

	private static Double yearDistance(Short year, Short year2) {
		if (year == null || year2 == null) {
			return THRESHOLD;
		}
		int diff = 0;
		int max = 0;
		int n = Math.max(CDUtility.getNumberOfDigits(year), CDUtility.getNumberOfDigits(year2));
		for (int i = 0; i < n; i++) {
			int weight = i + 1;
			max += weight * 9;
			diff += weight * Math.abs(CDUtility.getNthDigit(year, i) - CDUtility.getNthDigit(year2, i));
		}
		return 1 - (double) diff / max;
	}

	public CDUtility() {
		datasetThreshold = THRESHOLD;
	}

	/**
	 * Brute-force results:
	 *
	 * <ul>
	 * <li>Recall: 0.8304</li>
	 * <li>Precision: 0.8451</li>
	 * <li>F-Measure: 0.8377</li>
	 * </ul>
	 */
	@Override
	public Double calculateSimilarity(Map<String, Object> firstRecord, Map<String, Object> secondRecord,
			Map<String, String> parameters) {
		return similarity(CDRecordParser.parse(firstRecord), CDRecordParser.parse(secondRecord));
	}

	@Override
	public Map<String, Object> parseRecord(Map<String, String> value) {
		Map<String, Object> record = new HashMap<>();
		record.put(ID, value.get(ID));
		record.put(ARTIST_NAME, value.get(ARTIST_NAME));
		record.put(TITLE_NAME, value.get(TITLE_NAME));
		record.put(CATEGORY_NAME, value.get(CATEGORY_NAME));
		String year = value.get(YEAR_NAME);
		if (!year.isEmpty()) {
			record.put(YEAR_NAME, Short.parseShort(year));
		}
		record.put(GENRE_NAME, value.get(GENRE_NAME));
		record.put(CDEXTRA_NAME, value.get(CDEXTRA_NAME));
		record.put(TRACKS_NAME, Arrays.asList(value.get(TRACKS_NAME).split(SEPERATOR)).stream()
				.map(CDUtility::trimNumbers).map(CDUtility::normalize).collect(Collectors.toList()));
		return record;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Double compareAttributeValue(String attribute, Object value1, Object value2) {
		switch (attribute) {
		case ARTIST_NAME:
			return levenshteinDistance((String) value1, (String) value2);
		case TITLE_NAME:
			return levenshteinDistance((String) value1, (String) value2);
		case CATEGORY_NAME:
			return levenshteinDistance((String) value1, (String) value2);
		case GENRE_NAME:
			return levenshteinDistance((String) value1, (String) value2);
		case YEAR_NAME:
			return yearDistance((Short) value1, (Short) value2);
		case CDEXTRA_NAME:
			return levenshteinDistance((String) value1, (String) value2);
		case TRACKS_NAME:
			return getSimilarityOfTracks((List<String>) value1, (List<String>) value2);
		default:
			throw new IllegalArgumentException("Unknown attribute: " + attribute);
		}
	}

	@Override
	public Double calculateAttributeSimilarity(Map<String, Double> similarities) {
		double result = 0.0;
		for (Entry<String, Double> entry : similarities.entrySet()) {
			result += Attribute.getForName(entry.getKey()).weight() * entry.getValue();
		}
		return result / Attribute.TOTAL_WEIGHT;
	}
}
