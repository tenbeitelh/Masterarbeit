package de.hs.osnabrueck.tenbeitel.mr.association.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class AprioriUtils {

	public static Set<List<String>> generateCandidates(Set<List<String>> itemSets, int actualItemSetLength) {
		List<List<String>> itemSetsList = new ArrayList<List<String>>(itemSets);
		Set<List<String>> kItemSet = new HashSet<List<String>>();
		if (actualItemSetLength > 2) {
			for (int i = 0; i < itemSetsList.size(); i++) {
				for (int j = 0; j < itemSetsList.size(); j++) {

					if (!itemSetsList.get(i).equals(itemSetsList.get(j))) {

						ArrayList<String> firstArray = new ArrayList<String>(itemSetsList.get(i));
						ArrayList<String> secondArray = new ArrayList<String>(itemSetsList.get(j));

						List<String> subsetFirstArray = firstArray.subList(0, actualItemSetLength - 1);
						List<String> subsetSecondArray = secondArray.subList(0, actualItemSetLength - 1);

						if (subsetFirstArray.equals(subsetSecondArray)) {
							firstArray.add(secondArray.get(secondArray.size() - 1));

							if (checkCandidateSubset(itemSetsList, generateCandidateSubset(firstArray))) {
								kItemSet.add(firstArray);
							}

						}
					}
				}
			}
		} else {
			for (int i = 0; i < itemSetsList.size(); i++) {
				for (int j = 0; j < itemSetsList.size(); j++) {
					if (!itemSetsList.get(i).equals(itemSetsList.get(j))) {
						String[] newItemSet = new String[] { itemSetsList.get(i).get(0), itemSetsList.get(j).get(0) };
						Arrays.sort(newItemSet);
						kItemSet.add(Arrays.asList(newItemSet));
					}
				}
			}
		}
		return kItemSet;
	}

	private static String[][] generateCandidateSubset(List<String> firstArray) {
		String[][] comb = new String[firstArray.size()][firstArray.size() - 1];

		for (int i = 0; i < firstArray.size(); i++) {
			for (int j = 0; j < firstArray.size() - 1; j++) {
				if (i != j) {
					comb[i][j] = firstArray.get(j);
				} else {
					comb[i][j] = firstArray.get(firstArray.size() - 1);
				}
			}
		}

		return comb;

	}

	private static boolean checkCandidateSubset(List<List<String>> itemSet, String[][] subsets) {
		for (int i = 0; i < subsets.length; i++) {

			if (!itemSet.contains(Arrays.asList(subsets[i]))) {
				return false;
			}
		}
		return true;
	}

	public static boolean arrayContainsSubset(String[] array, String[] subset) {
		List<String> arrayList = Arrays.asList(array);
		List<String> subsetList = Arrays.asList(subset);
		return arrayList.containsAll(subsetList);
	}

	public static boolean arrayContainsSubset(List<String> array, String[] subset) {
		List<String> subsetList = Arrays.asList(subset);
		return array.containsAll(subsetList);
	}
}
