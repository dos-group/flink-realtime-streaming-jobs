package de.tuberlin.cit.test.twittersentiment.util;

import com.aliasi.classify.ConditionalClassification;
import com.aliasi.classify.LMClassifier;
import com.aliasi.util.AbstractExternalizable;

import java.io.IOException;

public class SentimentClassifier {
	String[] categories;
	LMClassifier classifier;

	public SentimentClassifier(String path) {
		try {
			classifier = (LMClassifier) AbstractExternalizable.readResourceObject(SentimentClassifier.class, path);
			categories = classifier.categories();
		} catch (ClassNotFoundException | IOException e) {
			e.printStackTrace();
		}
	}

	public SentimentClassifier() {
		this("classifier.txt");
	}

	public String classify(String text) {
		ConditionalClassification classification = classifier.classify(text);
		return classification.bestCategory();
	}
}
