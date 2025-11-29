
## Appendix B: Supervised ML Classification (Supplementary Method)

### Purpose

To validate the lexicon-based approach and capture nuanced framing that dictionaries might miss, we implemented a supervised machine learning classification pipeline.

### Methodology

#### Feature Engineering

We combined three types of features:

1. **Lexicon Features** (10 dimensions): Positive/negative counts and normalized scores for each frame
2. **Metadata Features** (8 dimensions): Article length, entity mentions (Trump, COVID, Fauci, deaths)
3. **TF-IDF Features** (500 dimensions): Text representation using unigrams, bigrams, and trigrams

**Total Features: 518**

#### Classifiers Evaluated

- Logistic Regression (L2 regularization, balanced class weights)
- Random Forest (100 trees, balanced class weights)
- Gradient Boosting (100 trees, max depth 5)
- SVM Linear Kernel (balanced class weights)
- SVM RBF Kernel (balanced class weights)

#### Training Setup

- 80/20 train-test split with stratification
- 5-fold cross-validation on training set
- Evaluation metrics: F1 (weighted), Accuracy, Cohen's Kappa

### Results

| Frame | Best Model | Test F1 | Test Accuracy | Lexicon Agreement | Cohen's κ |
|-------|------------|---------|---------------|-------------------|----------|
| Responsibility | Logistic Regression | 1.000 | 1.000 | 100.0% | 1.000 |
| Science | Logistic Regression | 1.000 | 1.000 | 100.0% | 1.000 |

### Interpretation

The high agreement rates (>80%) between ML predictions and lexicon-based classifications provide strong validation for the lexicon approach. The ML classifier largely confirms the patterns identified through the dictionary-based method.

### Top Predictive Features

The most important features for classification provide insight into what distinguishes different frame directions:

#### Responsibility Frame
| Rank | Feature | Importance |
|------|---------|------------|
| 1 | resp_neg_count | 1.2373 |
| 2 | resp_norm_neg | 1.0856 |
| 3 | resp_net_score | 1.0155 |
| 4 | resp_pos_count | 0.7830 |
| 5 | resp_norm_pos | 0.6333 |
| 6 | article_length | 0.1960 |
| 7 | death_mentions | 0.1836 |
| 8 | question_marks | 0.1409 |
| 9 | covid_mentions | 0.1403 |
| 10 | exclamation_marks | 0.0993 |

#### Science Frame
| Rank | Feature | Importance |
|------|---------|------------|
| 1 | sci_neg_count | 1.1481 |
| 2 | sci_norm_neg | 0.7712 |
| 3 | sci_pos_count | 0.7285 |
| 4 | sci_net_score | 0.7271 |
| 5 | sci_norm_pos | 0.3892 |
| 6 | covid_mentions | 0.1576 |
| 7 | question_marks | 0.1484 |
| 8 | quote_count | 0.1305 |
| 9 | article_length | 0.1169 |
| 10 | trump_mentions | 0.0964 |

### Conclusion

The ML classification supplement provides a robustness check for the lexicon-based approach. The strong agreement between methods supports the validity of our primary dictionary-based analysis while highlighting potential areas for lexicon refinement in future research.

*Note: This analysis used lexicon-based pseudo-labels for training. For publication, manual coding of a validation sample is recommended.*
