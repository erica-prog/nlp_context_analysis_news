"""
Two-Dimension Framing Analysis: Responsibility & Science Frames
Trump COVID-19 Coverage: NYT vs. Guardian

Implements lexicon-based approach with ML supplement for validation.

Research Questions:
- RQ1: How do NYT and The Guardian differ in attributing responsibility 
       for COVID-19 outcomes during the Trump administration?
- RQ2: How do the two outlets frame Trump's relationship with scientific 
       expertise during the pandemic?

Methodology based on Entman (1993) framing theory.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Set, Optional
import re
from collections import defaultdict
from pathlib import Path
import warnings

warnings.filterwarnings('ignore')


# ============================================================================
# PART 1: FRAME LEXICONS (REFINED & FOCUSED)
# ============================================================================

class FrameLexicons:
    """
    Frame-specific lexicons for responsibility and science dimensions.
    
    Design rationale:
    - Unlike sentiment analysis (VADER), these lexicons directly operationalize
      responsibility attribution and science engagement frames (Entman, 1993)
    - Focus on causality assignment and credibility framing
    - Terms validated through manual coding of 100-article sample
    """
    
    # ========== RESPONSIBILITY FRAME ==========
    # Measures: Who gets blamed/credited for COVID outcomes?
    
    RESPONSIBILITY_POSITIVE = {
        # Leadership terms (credit attribution)
        'leadership', 'led the response', 'took charge', 'decisive', 
        'decisive action', 'bold action', 'swift response', 'quick action',
        'acted quickly', 'immediate action', 'strong leadership',
        'took responsibility', 'stepped up', 'rose to the challenge',
        
        # Achievement terms (success attribution)
        'credited with', 'praised for', 'effective response', 'successful',
        'accomplishment', 'delivered', 'achievement', 'managed well',
        'under control', 'got it under control', 'controlled the outbreak',
        'handled effectively', 'competent response', 'saved lives',
        
        # Coordination terms (organizational credit)
        'coordinated response', 'national strategy', 'federal coordination',
        'mobilized', 'organized effort', 'task force', 'unified response',
        'working together', 'coordinated effort', 'effective coordination',
        
        # Specific COVID achievements (Trump admin claims)
        'operation warp speed', 'warp speed', 'vaccine development',
        'vaccine delivery', 'rapid vaccine', 'delivered vaccines',
        'vaccine success', 'record time', 'unprecedented speed',
        'historic achievement', 'fastest vaccine', 'vaccine miracle'
    }
    
    RESPONSIBILITY_NEGATIVE = {
        # Failure terms (blame attribution)
        'failed', 'failure', 'failed to act', 'failed to respond',
        'negligent', 'negligence', 'mismanaged', 'mismanagement',
        'bungled', 'botched', 'fumbled', 'disaster', 'fiasco',
        'incompetent', 'incompetence', 'reckless', 'dereliction',
        
        # Warning/delay terms (inaction blame)
        'ignored warnings', 'dismissed warnings', 'early warnings ignored',
        'downplayed', 'downplayed the threat', 'minimized', 'minimized the risk',
        'delayed response', 'slow response', 'late response', 'slow to act',
        'months of delay', 'crucial weeks', 'lost time', 'wasted time',
        'squandered time', 'precious weeks', 'critical window',
        
        # Accountability terms (direct blame)
        'responsible for deaths', 'preventable deaths', 'avoidable deaths',
        'unnecessary deaths', 'blood on his hands', 'could have prevented',
        'should have acted', 'could have saved', 'if he had acted',
        'lives lost', 'needless deaths', 'tragic failure',
        
        # Chaos/planning terms (systemic critique)
        'no plan', 'no national plan', 'lack of plan', 'no strategy',
        'no national strategy', 'chaotic', 'chaos', 'disorganized',
        'unprepared', 'unpreparedness', 'caught unprepared',
        'abdicated', 'abdicated responsibility', 'abdication',
        'lack of leadership', 'leadership vacuum', 'absence of leadership',
        
        # Severity/impact terms (consequence framing)
        'catastrophic', 'catastrophic failure', 'crisis mismanagement',
        'toll', 'death toll', 'rising deaths', 'mounting deaths',
        'casualties', 'hundreds of thousands dead', 'mass death',
        'surging cases', 'spiraling', 'out of control',
        
        # Blame language (explicit attribution)
        'to blame', 'blamed for', 'fault', 'at fault', 'culpable',
        'accountability', 'held accountable', 'must answer for',
        'bears responsibility', 'share of blame', 'falls on'
    }
    
    # ========== SCIENCE/EXPERTISE FRAME ==========
    # Measures: How is Trump's relationship with science portrayed?
    
    SCIENCE_POSITIVE = {
        # Expert consultation (deference to expertise)
        'followed cdc', 'cdc guidelines', 'followed guidelines',
        'listened to experts', 'consulting experts', 'consulted scientists',
        'expert advisors', 'scientific advisors', 'health advisors',
        'sought expert advice', 'deferred to experts', 'heeded advice',
        'followed recommendations', 'respected expertise',
        
        # Evidence-based decision making
        'evidence-based', 'science-based', 'data-driven', 'research-based',
        'based on science', 'scientific approach', 'follows the science',
        'guided by science', 'let science guide', 'scientific evidence',
        'backed by research', 'supported by data',
        
        # Positive expert relationships
        'dr fauci', 'anthony fauci', 'dr birx', 'deborah birx',
        'cdc director', 'surgeon general', 'nih', 'niaid',
        'national institutes', 'public health experts', 'epidemiologists',
        'respects experts', 'trusts scientists', 'works with scientists',
        
        # Vaccine development (scientific success framing)
        'operation warp speed', 'warp speed success', 
        'vaccine development', 'vaccine research', 'clinical trials',
        'funded research', 'research funding', 'scientific breakthrough',
        'public-private partnership', 'accelerated development'
    }
    
    SCIENCE_NEGATIVE = {
        # Rejection of science (anti-expertise framing)
        'rejected science', 'rejected expert advice', 'ignored science',
        'ignored experts', 'dismissed experts', 'dismisses science',
        'goes against science', 'contradicts science', 'defies science',
        'overruled scientists', 'disregards evidence', 'anti-science',
        
        # Firing/silencing experts (suppression framing)
        'fired experts', 'removed experts', 'sidelined scientists',
        'muzzled scientists', 'gagged scientists', 'censored scientists',
        'silenced experts', 'blocked scientists', 'stopped scientists',
        'pushed out', 'marginalized experts', 'undermined scientists',
        
        # Misinformation (credibility attack framing)
        'misinformation', 'spreads misinformation', 'disinformation',
        'false information', 'conspiracy theory', 'conspiracy theories',
        'promotes conspiracy', 'pseudoscience', 'unproven', 'debunked',
        'false claims', 'misleading claims', 'unfounded claims',
        'without evidence', 'no scientific basis', 'baseless',
        'medically unfounded', 'scientifically unsound',
        
        # Specific dangerous claims (concrete examples)
        'hydroxychloroquine', 'bleach', 'disinfectant', 'inject bleach',
        'inject disinfectant', 'drink bleach', 'unproven treatments',
        'unproven therapies', 'quack cures', 'snake oil',
        'miracle cure', 'unapproved treatments', 'dangerous advice',
        
        # Conflicts with health officials (institutional conflict)
        'contradicted fauci', 'contradicts fauci', 'attacked fauci',
        'attacks fauci', 'undermined fauci', 'undermines fauci',
        'sidelined fauci', 'ignored fauci', 'dismissed fauci',
        'contradicted cdc', 'contradicts cdc', 'undermined cdc',
        'undermines cdc', 'overruled cdc', 'ignored cdc',
        'against cdc advice', 'defied cdc', 'clashed with',
        
        # Anti-science behaviors (behavioral framing)
        'anti-mask', 'refused mask', 'wont wear mask', 'mocked masks',
        'ridiculed masks', 'mask skepticism', 'anti-vaccine',
        'vaccine skepticism', 'vaccine hesitancy', 'mask resistance',
        
        # Pandemic denial framing
        'hoax', 'democratic hoax', 'political hoax', 'witch hunt',
        'fake news', 'media hoax', 'overblown', 'overhyped',
        'exaggerated threat', 'exaggerated', 'just the flu',
        'no worse than flu', 'will disappear', 'like magic',
        'totally under control', 'nothing to worry about'
    }
    
    @classmethod
    def get_all_terms(cls) -> Dict[str, Set[str]]:
        """Return all lexicons as a dictionary."""
        return {
            'responsibility_positive': cls.RESPONSIBILITY_POSITIVE,
            'responsibility_negative': cls.RESPONSIBILITY_NEGATIVE,
            'science_positive': cls.SCIENCE_POSITIVE,
            'science_negative': cls.SCIENCE_NEGATIVE
        }
    
    @classmethod
    def get_term_counts(cls) -> Dict[str, int]:
        """Return counts of terms in each lexicon."""
        return {
            'responsibility_positive': len(cls.RESPONSIBILITY_POSITIVE),
            'responsibility_negative': len(cls.RESPONSIBILITY_NEGATIVE),
            'science_positive': len(cls.SCIENCE_POSITIVE),
            'science_negative': len(cls.SCIENCE_NEGATIVE)
        }


# ============================================================================
# PART 2: FRAME DETECTION ENGINE
# ============================================================================

class TwoDimensionFrameDetector:
    """
    Detects responsibility and science frames using lexicon-based approach.
    
    Score calculation: (positive matches - negative matches) / total words
    This normalizes by document length to allow cross-article comparison.
    """
    
    def __init__(self):
        self.lexicons = FrameLexicons()
        # Precompile regex patterns for efficiency
        self._compiled_patterns = {}
        self._compile_patterns()
        
    def _compile_patterns(self):
        """Pre-compile regex patterns for all lexicon terms."""
        all_lexicons = self.lexicons.get_all_terms()
        for lex_name, terms in all_lexicons.items():
            self._compiled_patterns[lex_name] = {}
            for term in terms:
                # Create word-boundary pattern for each term
                pattern = r'\b' + re.escape(term.lower()) + r'\b'
                self._compiled_patterns[lex_name][term] = re.compile(pattern)
        
    def preprocess_text(self, text: str) -> str:
        """Clean and normalize text for matching."""
        if pd.isna(text):
            return ""
        
        text = str(text).lower()
        # Remove HTML tags
        text = re.sub(r'<[^>]+>', ' ', text)
        # Keep apostrophes for contractions, remove other punctuation
        text = re.sub(r"[^a-z0-9\s\-']", ' ', text)
        # Normalize whitespace
        text = ' '.join(text.split())
        return text
    
    def count_phrase_matches(self, text: str, lexicon_name: str) -> Tuple[int, List[str]]:
        """
        Count how many lexicon terms appear in text using compiled patterns.
        Returns: (count, list of matched terms)
        """
        text = self.preprocess_text(text)
        matches = []
        
        patterns = self._compiled_patterns.get(lexicon_name, {})
        for term, pattern in patterns.items():
            if pattern.search(text):
                matches.append(term)
        
        return len(matches), matches
    
    def calculate_frame_score(self, text: str, frame_type: str) -> Dict:
        """
        Calculate frame scores for a single text.
        
        Args:
            text: The article text to analyze
            frame_type: Either 'responsibility' or 'science'
        
        Returns dictionary with:
            - pos_count: Number of positive terms matched
            - neg_count: Number of negative terms matched
            - pos_terms: List of positive terms matched
            - neg_terms: List of negative terms matched
            - norm_pos: Normalized positive score (per 1000 words)
            - norm_neg: Normalized negative score (per 1000 words)
            - net_score: (pos - neg) / total_words
            - direction: 'positive', 'negative', 'neutral', or 'absent'
            - intensity: Absolute magnitude of framing
        """
        
        pos_lex_name = f'{frame_type}_positive'
        neg_lex_name = f'{frame_type}_negative'
        
        # Count matches
        pos_count, pos_terms = self.count_phrase_matches(text, pos_lex_name)
        neg_count, neg_terms = self.count_phrase_matches(text, neg_lex_name)
        
        # Get word count
        words = self.preprocess_text(text).split()
        total_words = len(words) if len(words) > 0 else 1
        
        # Normalize by document length (per 1000 words for readability)
        norm_pos = (pos_count / total_words) * 1000
        norm_neg = (neg_count / total_words) * 1000
        
        # Calculate net score (raw, not scaled)
        net_score = (pos_count - neg_count) / total_words
        
        # Calculate intensity (total frame presence)
        intensity = (pos_count + neg_count) / total_words
        
        # Determine direction with threshold
        # Threshold: at least 0.001 difference (1 term per 1000 words)
        threshold = 0.001
        
        if pos_count == 0 and neg_count == 0:
            direction = 'absent'
        elif net_score > threshold:
            direction = 'positive'
        elif net_score < -threshold:
            direction = 'negative'
        else:
            direction = 'neutral'
        
        return {
            'pos_count': pos_count,
            'neg_count': neg_count,
            'pos_terms': pos_terms,
            'neg_terms': neg_terms,
            'norm_pos': norm_pos,
            'norm_neg': norm_neg,
            'net_score': net_score,
            'direction': direction,
            'intensity': intensity,
            'word_count': total_words
        }
    
    def detect_both_frames(self, text: str) -> Dict:
        """
        Detect both responsibility and science frames.
        Returns dictionary with results for both dimensions.
        """
        return {
            'responsibility': self.calculate_frame_score(text, 'responsibility'),
            'science': self.calculate_frame_score(text, 'science')
        }


# ============================================================================
# PART 3: VALIDATION FRAMEWORK
# ============================================================================

class ValidationFramework:
    """
    Framework for validating lexicon-based analysis against manual coding.
    Supports inter-rater reliability calculation and ML supplement.
    """
    
    def __init__(self):
        self.manual_codings = None
        self.validation_results = None
        
    def load_manual_codings(self, filepath: str) -> pd.DataFrame:
        """
        Load manually coded articles for validation.
        
        Expected columns:
        - article_id: Unique identifier
        - text: Article text
        - resp_manual: Manual responsibility coding (-1, 0, 1)
        - sci_manual: Manual science coding (-1, 0, 1)
        - coder_id: ID of human coder (for IRR calculation)
        """
        self.manual_codings = pd.read_csv(filepath)
        return self.manual_codings
    
    def calculate_agreement(self, lexicon_scores: pd.DataFrame) -> Dict:
        """
        Calculate agreement between lexicon scores and manual coding.
        
        Returns:
            - accuracy: Proportion of matching classifications
            - kappa: Cohen's Kappa for inter-method agreement
            - confusion_matrix: Detailed breakdown
        """
        from sklearn.metrics import cohen_kappa_score, accuracy_score, confusion_matrix
        
        results = {}
        
        for frame in ['resp', 'sci']:
            manual_col = f'{frame}_manual'
            lexicon_col = f'{frame}_direction'
            
            if manual_col not in lexicon_scores.columns:
                continue
                
            # Convert direction to numeric
            direction_map = {'negative': -1, 'neutral': 0, 'positive': 1, 'absent': 0}
            lexicon_numeric = lexicon_scores[lexicon_col].map(direction_map)
            manual_numeric = lexicon_scores[manual_col]
            
            # Calculate metrics
            acc = accuracy_score(manual_numeric, lexicon_numeric)
            kappa = cohen_kappa_score(manual_numeric, lexicon_numeric)
            cm = confusion_matrix(manual_numeric, lexicon_numeric, labels=[-1, 0, 1])
            
            results[frame] = {
                'accuracy': acc,
                'kappa': kappa,
                'confusion_matrix': cm,
                'n_samples': len(lexicon_scores)
            }
        
        self.validation_results = results
        return results
    
    def calculate_inter_rater_reliability(self, codings: pd.DataFrame) -> Dict:
        """
        Calculate inter-rater reliability for manual codings.
        
        Args:
            codings: DataFrame with columns: article_id, coder_id, resp_code, sci_code
        """
        from sklearn.metrics import cohen_kappa_score
        
        # Pivot to get coders as columns
        results = {}
        
        for frame in ['resp', 'sci']:
            col = f'{frame}_code'
            pivot = codings.pivot(index='article_id', columns='coder_id', values=col)
            
            if pivot.shape[1] >= 2:
                coder1, coder2 = pivot.columns[:2]
                valid_rows = pivot[[coder1, coder2]].dropna()
                
                kappa = cohen_kappa_score(valid_rows[coder1], valid_rows[coder2])
                pct_agree = (valid_rows[coder1] == valid_rows[coder2]).mean()
                
                results[frame] = {
                    'kappa': kappa,
                    'percent_agreement': pct_agree,
                    'n_double_coded': len(valid_rows)
                }
        
        return results


# ============================================================================
# PART 4: ML CLASSIFIER SUPPLEMENT
# ============================================================================

class MLClassifierSupplement:
    """
    Machine learning classifier to supplement lexicon-based analysis.
    Uses TF-IDF features combined with lexicon scores.
    
    Purpose: Provides robustness check for lexicon-based findings.
    """
    
    def __init__(self):
        self.vectorizer = None
        self.classifiers = {}
        self.feature_names = None
        
    def prepare_features(self, texts: pd.Series, lexicon_scores: pd.DataFrame) -> np.ndarray:
        """
        Prepare feature matrix combining TF-IDF and lexicon scores.
        
        Features:
        1. TF-IDF of article text (top 500 features)
        2. Lexicon match counts (pos/neg for each frame)
        3. Lexicon intensity scores
        """
        from sklearn.feature_extraction.text import TfidfVectorizer
        
        # TF-IDF features
        if self.vectorizer is None:
            self.vectorizer = TfidfVectorizer(
                max_features=500,
                stop_words='english',
                ngram_range=(1, 2),
                min_df=5,
                max_df=0.95
            )
            tfidf_matrix = self.vectorizer.fit_transform(texts.fillna(''))
        else:
            tfidf_matrix = self.vectorizer.transform(texts.fillna(''))
        
        # Lexicon features
        lexicon_features = lexicon_scores[[
            'resp_pos_count', 'resp_neg_count', 'resp_net_score',
            'sci_pos_count', 'sci_neg_count', 'sci_net_score'
        ]].values
        
        # Combine features
        from scipy.sparse import hstack, csr_matrix
        combined = hstack([tfidf_matrix, csr_matrix(lexicon_features)])
        
        return combined
    
    def train_classifier(self, X: np.ndarray, y: np.ndarray, frame: str) -> Dict:
        """
        Train a classifier for a specific frame.
        
        Args:
            X: Feature matrix
            y: Labels (-1, 0, 1)
            frame: 'resp' or 'sci'
        
        Returns:
            Training results and cross-validation scores
        """
        from sklearn.linear_model import LogisticRegression
        from sklearn.model_selection import cross_val_score, StratifiedKFold
        
        # Use logistic regression for interpretability
        clf = LogisticRegression(
            max_iter=1000,
            class_weight='balanced',
            random_state=42
        )
        
        # Cross-validation
        cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
        scores = cross_val_score(clf, X, y, cv=cv, scoring='accuracy')
        
        # Fit on full data
        clf.fit(X, y)
        self.classifiers[frame] = clf
        
        return {
            'cv_accuracy_mean': scores.mean(),
            'cv_accuracy_std': scores.std(),
            'cv_scores': scores
        }
    
    def compare_with_lexicon(self, X: np.ndarray, lexicon_predictions: np.ndarray, 
                             true_labels: np.ndarray, frame: str) -> Dict:
        """
        Compare ML predictions with lexicon-based predictions.
        
        Returns agreement statistics and areas of disagreement.
        """
        from sklearn.metrics import accuracy_score, cohen_kappa_score
        
        clf = self.classifiers.get(frame)
        if clf is None:
            raise ValueError(f"No trained classifier for frame: {frame}")
        
        ml_predictions = clf.predict(X)
        
        # Agreement between methods
        method_agreement = (ml_predictions == lexicon_predictions).mean()
        method_kappa = cohen_kappa_score(ml_predictions, lexicon_predictions)
        
        # Accuracy of each method (if true labels available)
        if true_labels is not None:
            ml_accuracy = accuracy_score(true_labels, ml_predictions)
            lex_accuracy = accuracy_score(true_labels, lexicon_predictions)
        else:
            ml_accuracy = None
            lex_accuracy = None
        
        return {
            'method_agreement': method_agreement,
            'method_kappa': method_kappa,
            'ml_accuracy': ml_accuracy,
            'lexicon_accuracy': lex_accuracy,
            'n_disagreements': (ml_predictions != lexicon_predictions).sum()
        }


# ============================================================================
# PART 5: ANALYSIS PIPELINE
# ============================================================================

class FramingAnalysisPipeline:
    """
    Complete pipeline for two-dimension framing analysis.
    
    Workflow:
    1. Load and filter data
    2. Apply lexicon-based frame detection
    3. Generate summary statistics
    4. Run statistical tests
    5. (Optional) Validate with ML classifier
    6. Export results
    """
    
    def __init__(self, nyt_path: str = None, guardian_path: str = None):
        self.detector = TwoDimensionFrameDetector()
        self.validator = ValidationFramework()
        self.ml_supplement = MLClassifierSupplement()
        self.df = None
        self.project_root = Path(__file__).parent.parent
        
        if nyt_path and guardian_path:
            self.load_and_prepare_data(nyt_path, guardian_path)
    
    def load_and_prepare_data(self, nyt_path: str, guardian_path: str) -> pd.DataFrame:
        """Load and filter data from CSV files."""
        
        print("Loading datasets...")
        nyt = pd.read_csv(nyt_path)
        guardian = pd.read_csv(guardian_path)
        
        # Add outlet identifier
        nyt['outlet'] = 'NYT'
        guardian['outlet'] = 'Guardian'
        
        # Combine text fields (headline + abstract/snippet)
        nyt['text'] = (nyt['headline'].fillna('') + ' ' + 
                      nyt.get('abstract', nyt.get('snippet', '')).fillna(''))
        guardian['text'] = (guardian['headline'].fillna('') + ' ' + 
                           guardian.get('abstract', guardian.get('snippet', '')).fillna(''))
        
        # Convert dates
        nyt['pub_date'] = pd.to_datetime(nyt['pub_date'], utc=True)
        guardian['pub_date'] = pd.to_datetime(guardian['pub_date'], utc=True)
        
        # Filter for Trump administration period (Jan 20, 2020 - Jan 20, 2021)
        print("Filtering for Trump COVID period (Jan 2020 - Jan 2021)...")
        start_date = pd.Timestamp('2020-01-20', tz='UTC')
        end_date = pd.Timestamp('2021-01-20', tz='UTC')
        
        nyt = nyt[(nyt['pub_date'] >= start_date) & (nyt['pub_date'] < end_date)]
        guardian = guardian[(guardian['pub_date'] >= start_date) & 
                           (guardian['pub_date'] < end_date)]
        
        # Filter for COVID + Trump mentions
        print("Filtering for COVID + Trump articles...")
        
        def contains_covid_trump(text):
            text_lower = str(text).lower()
            covid_terms = ['covid', 'coronavirus', 'pandemic', 'virus outbreak']
            trump_terms = ['trump', 'president trump', 'administration']
            
            has_covid = any(term in text_lower for term in covid_terms)
            has_trump = any(term in text_lower for term in trump_terms)
            
            return has_covid and has_trump
        
        nyt = nyt[nyt['text'].apply(contains_covid_trump)]
        guardian = guardian[guardian['text'].apply(contains_covid_trump)]
        
        # Combine datasets
        self.df = pd.concat([nyt, guardian], ignore_index=True)
        
        # Add month column for temporal analysis
        self.df['month'] = self.df['pub_date'].dt.to_period('M')
        
        print(f"\n{'='*50}")
        print("DATASET SUMMARY")
        print(f"{'='*50}")
        print(f"  Total articles: {len(self.df):,}")
        print(f"  NYT articles: {len(nyt):,}")
        print(f"  Guardian articles: {len(guardian):,}")
        print(f"  Date range: {self.df['pub_date'].min().date()} to {self.df['pub_date'].max().date()}")
        print(f"{'='*50}\n")
        
        return self.df
    
    def load_from_project_data(self) -> pd.DataFrame:
        """Load data from the project's data directories."""
        
        nyt_combined = self.project_root / 'nytimes_trump_covid' / 'combined.csv'
        guardian_combined = self.project_root / 'guardian_trump_covid' / 'combined.csv'
        
        if nyt_combined.exists() and guardian_combined.exists():
            return self.load_and_prepare_data(str(nyt_combined), str(guardian_combined))
        else:
            # Load individual monthly files
            print("Loading individual monthly files...")
            
            nyt_files = list((self.project_root / 'nytimes_trump_covid').glob('2020-*.csv'))
            nyt_files += list((self.project_root / 'nytimes_trump_covid').glob('2021-01.csv'))
            
            guardian_files = list((self.project_root / 'guardian_trump_covid').glob('2020-*.csv'))
            guardian_files += list((self.project_root / 'guardian_trump_covid').glob('2021-01.csv'))
            
            nyt_dfs = [pd.read_csv(f) for f in nyt_files if f.exists()]
            guardian_dfs = [pd.read_csv(f) for f in guardian_files if f.exists()]
            
            nyt = pd.concat(nyt_dfs, ignore_index=True) if nyt_dfs else pd.DataFrame()
            guardian = pd.concat(guardian_dfs, ignore_index=True) if guardian_dfs else pd.DataFrame()
            
            # Save combined for future use
            if not nyt.empty:
                nyt.to_csv(nyt_combined, index=False)
            if not guardian.empty:
                guardian.to_csv(guardian_combined, index=False)
            
            return self.load_and_prepare_data(str(nyt_combined), str(guardian_combined))
    
    def analyze_frames(self, progress_interval: int = 100) -> pd.DataFrame:
        """Apply frame detection to all articles."""
        
        if self.df is None:
            raise ValueError("No data loaded. Call load_and_prepare_data() first.")
        
        print("Analyzing frames...")
        
        # Initialize columns
        frame_columns = []
        for frame in ['resp', 'sci']:
            for col in ['pos_count', 'neg_count', 'norm_pos', 'norm_neg', 
                       'net_score', 'direction', 'intensity', 'pos_terms', 'neg_terms']:
                col_name = f'{frame}_{col}'
                frame_columns.append(col_name)
                if col in ['pos_count', 'neg_count']:
                    self.df[col_name] = 0
                elif col in ['norm_pos', 'norm_neg', 'net_score', 'intensity']:
                    self.df[col_name] = 0.0
                else:
                    self.df[col_name] = ''
        
        # Analyze each article
        for idx, row in self.df.iterrows():
            if idx % progress_interval == 0:
                print(f"  Processed {idx:,}/{len(self.df):,} articles...")
            
            # Detect frames
            frames = self.detector.detect_both_frames(row['text'])
            
            # Store responsibility frame results
            resp = frames['responsibility']
            self.df.at[idx, 'resp_pos_count'] = resp['pos_count']
            self.df.at[idx, 'resp_neg_count'] = resp['neg_count']
            self.df.at[idx, 'resp_norm_pos'] = resp['norm_pos']
            self.df.at[idx, 'resp_norm_neg'] = resp['norm_neg']
            self.df.at[idx, 'resp_net_score'] = resp['net_score']
            self.df.at[idx, 'resp_direction'] = resp['direction']
            self.df.at[idx, 'resp_intensity'] = resp['intensity']
            self.df.at[idx, 'resp_pos_terms'] = '; '.join(resp['pos_terms'][:5])
            self.df.at[idx, 'resp_neg_terms'] = '; '.join(resp['neg_terms'][:5])
            
            # Store science frame results
            sci = frames['science']
            self.df.at[idx, 'sci_pos_count'] = sci['pos_count']
            self.df.at[idx, 'sci_neg_count'] = sci['neg_count']
            self.df.at[idx, 'sci_norm_pos'] = sci['norm_pos']
            self.df.at[idx, 'sci_norm_neg'] = sci['norm_neg']
            self.df.at[idx, 'sci_net_score'] = sci['net_score']
            self.df.at[idx, 'sci_direction'] = sci['direction']
            self.df.at[idx, 'sci_intensity'] = sci['intensity']
            self.df.at[idx, 'sci_pos_terms'] = '; '.join(sci['pos_terms'][:5])
            self.df.at[idx, 'sci_neg_terms'] = '; '.join(sci['neg_terms'][:5])
        
        print(f"  ✓ Completed {len(self.df):,} articles!")
        return self.df
    
    def generate_summary_statistics(self) -> Dict:
        """Generate detailed summary statistics by outlet."""
        
        summary = {}
        
        for outlet in ['NYT', 'Guardian']:
            outlet_df = self.df[self.df['outlet'] == outlet]
            
            summary[outlet] = {
                'n_articles': len(outlet_df),
                
                # Responsibility frame
                'resp_avg_net': outlet_df['resp_net_score'].mean(),
                'resp_std_net': outlet_df['resp_net_score'].std(),
                'resp_median_net': outlet_df['resp_net_score'].median(),
                'resp_direction_dist': outlet_df['resp_direction'].value_counts().to_dict(),
                'resp_pct_negative': (outlet_df['resp_direction'] == 'negative').mean() * 100,
                'resp_pct_positive': (outlet_df['resp_direction'] == 'positive').mean() * 100,
                'resp_pct_neutral': (outlet_df['resp_direction'] == 'neutral').mean() * 100,
                'resp_pct_absent': (outlet_df['resp_direction'] == 'absent').mean() * 100,
                'resp_avg_intensity': outlet_df['resp_intensity'].mean(),
                
                # Science frame
                'sci_avg_net': outlet_df['sci_net_score'].mean(),
                'sci_std_net': outlet_df['sci_net_score'].std(),
                'sci_median_net': outlet_df['sci_net_score'].median(),
                'sci_direction_dist': outlet_df['sci_direction'].value_counts().to_dict(),
                'sci_pct_negative': (outlet_df['sci_direction'] == 'negative').mean() * 100,
                'sci_pct_positive': (outlet_df['sci_direction'] == 'positive').mean() * 100,
                'sci_pct_neutral': (outlet_df['sci_direction'] == 'neutral').mean() * 100,
                'sci_pct_absent': (outlet_df['sci_direction'] == 'absent').mean() * 100,
                'sci_avg_intensity': outlet_df['sci_intensity'].mean(),
            }
        
        return summary
    
    def print_summary(self):
        """Print formatted summary statistics aligned with research questions."""
        
        stats = self.generate_summary_statistics()
        
        print("\n" + "="*70)
        print("FRAMING ANALYSIS RESULTS")
        print("Trump COVID-19 Coverage: NYT vs. Guardian (Jan 2020 - Jan 2021)")
        print("="*70)
        
        # Lexicon summary
        print("\nLEXICON SUMMARY:")
        term_counts = FrameLexicons.get_term_counts()
        print(f"  Responsibility frame: {term_counts['responsibility_positive']} positive, "
              f"{term_counts['responsibility_negative']} negative terms")
        print(f"  Science frame: {term_counts['science_positive']} positive, "
              f"{term_counts['science_negative']} negative terms")
        
        for outlet in ['NYT', 'Guardian']:
            s = stats[outlet]
            print(f"\n{'─'*70}")
            print(f"  {outlet.upper()} (N = {s['n_articles']:,} articles)")
            print(f"{'─'*70}")
            
            print(f"\n  RQ1: RESPONSIBILITY FRAME")
            print(f"  (Who gets blamed/credited for COVID-19 outcomes?)")
            print(f"    Average Net Score: {s['resp_avg_net']:.6f} (SD: {s['resp_std_net']:.6f})")
            print(f"    Median Net Score:  {s['resp_median_net']:.6f}")
            print(f"    Average Intensity: {s['resp_avg_intensity']:.6f}")
            print(f"\n    Direction Distribution:")
            print(f"      • Negative (blame): {s['resp_pct_negative']:.1f}%")
            print(f"      • Positive (credit): {s['resp_pct_positive']:.1f}%")
            print(f"      • Neutral: {s['resp_pct_neutral']:.1f}%")
            print(f"      • Absent: {s['resp_pct_absent']:.1f}%")
            
            print(f"\n  RQ2: SCIENCE FRAME")
            print(f"  (How is Trump's relationship with science portrayed?)")
            print(f"    Average Net Score: {s['sci_avg_net']:.6f} (SD: {s['sci_std_net']:.6f})")
            print(f"    Median Net Score:  {s['sci_median_net']:.6f}")
            print(f"    Average Intensity: {s['sci_avg_intensity']:.6f}")
            print(f"\n    Direction Distribution:")
            print(f"      • Negative (anti-science): {s['sci_pct_negative']:.1f}%")
            print(f"      • Positive (pro-science): {s['sci_pct_positive']:.1f}%")
            print(f"      • Neutral: {s['sci_pct_neutral']:.1f}%")
            print(f"      • Absent: {s['sci_pct_absent']:.1f}%")
        
        print("\n" + "="*70)
    
    def run_statistical_tests(self) -> Dict:
        """Run comprehensive statistical comparisons between outlets."""
        
        from scipy import stats as scipy_stats
        
        print("\n" + "="*70)
        print("STATISTICAL TESTS")
        print("="*70)
        
        nyt_df = self.df[self.df['outlet'] == 'NYT']
        guardian_df = self.df[self.df['outlet'] == 'Guardian']
        
        results = {}
        
        for frame, frame_name in [('resp', 'RESPONSIBILITY'), ('sci', 'SCIENCE')]:
            print(f"\n{frame_name} FRAME:")
            print("-" * 50)
            
            net_col = f'{frame}_net_score'
            dir_col = f'{frame}_direction'
            
            nyt_scores = nyt_df[net_col].dropna()
            guardian_scores = guardian_df[net_col].dropna()
            
            # Independent samples t-test
            t_stat, p_val = scipy_stats.ttest_ind(nyt_scores, guardian_scores)
            
            # Mann-Whitney U test (non-parametric alternative)
            u_stat, u_pval = scipy_stats.mannwhitneyu(nyt_scores, guardian_scores, alternative='two-sided')
            
            # Effect size: Cohen's d
            n1, n2 = len(nyt_scores), len(guardian_scores)
            var1 = nyt_scores.var()
            var2 = guardian_scores.var()
            pooled_std = np.sqrt(((n1-1)*var1 + (n2-1)*var2) / (n1+n2-2))
            cohens_d = (nyt_scores.mean() - guardian_scores.mean()) / pooled_std if pooled_std > 0 else 0
            
            # Effect size interpretation
            if abs(cohens_d) < 0.2:
                effect_interp = "negligible"
            elif abs(cohens_d) < 0.5:
                effect_interp = "small"
            elif abs(cohens_d) < 0.8:
                effect_interp = "medium"
            else:
                effect_interp = "large"
            
            print(f"  Continuous Score Comparison:")
            print(f"    NYT mean: {nyt_scores.mean():.6f} (SD: {nyt_scores.std():.6f})")
            print(f"    Guardian mean: {guardian_scores.mean():.6f} (SD: {guardian_scores.std():.6f})")
            print(f"    t-test: t = {t_stat:.3f}, p = {p_val:.4f} {'***' if p_val < 0.001 else '**' if p_val < 0.01 else '*' if p_val < 0.05 else ''}")
            print(f"    Mann-Whitney U: U = {u_stat:.0f}, p = {u_pval:.4f}")
            print(f"    Cohen's d: {cohens_d:.3f} ({effect_interp} effect)")
            
            # Chi-square test for direction distribution
            contingency = pd.crosstab(self.df['outlet'], self.df[dir_col])
            chi2, p, dof, expected = scipy_stats.chi2_contingency(contingency)
            
            # Cramér's V for effect size
            n = contingency.sum().sum()
            min_dim = min(contingency.shape) - 1
            cramers_v = np.sqrt(chi2 / (n * min_dim)) if min_dim > 0 else 0
            
            print(f"\n  Direction Distribution Comparison:")
            print(f"    Chi-square: χ² = {chi2:.2f}, df = {dof}, p = {p:.4f} {'***' if p < 0.001 else '**' if p < 0.01 else '*' if p < 0.05 else ''}")
            print(f"    Cramér's V: {cramers_v:.3f}")
            
            results[frame] = {
                't_stat': t_stat,
                't_pval': p_val,
                'u_stat': u_stat,
                'u_pval': u_pval,
                'cohens_d': cohens_d,
                'chi2': chi2,
                'chi2_pval': p,
                'chi2_dof': dof,
                'cramers_v': cramers_v,
                'nyt_mean': nyt_scores.mean(),
                'guardian_mean': guardian_scores.mean()
            }
        
        print("\n" + "="*70)
        print("Note: * p<0.05, ** p<0.01, *** p<0.001")
        print("="*70)
        
        return results
    
    def temporal_analysis(self) -> pd.DataFrame:
        """Analyze how framing changes over time (monthly trends)."""
        
        print("\n" + "="*70)
        print("TEMPORAL ANALYSIS")
        print("="*70)
        
        monthly_stats = self.df.groupby(['month', 'outlet']).agg({
            'resp_net_score': ['mean', 'std', 'count'],
            'sci_net_score': ['mean', 'std', 'count'],
            'resp_direction': lambda x: (x == 'negative').mean(),
            'sci_direction': lambda x: (x == 'negative').mean()
        }).round(4)
        
        monthly_stats.columns = ['_'.join(col).strip() for col in monthly_stats.columns.values]
        monthly_stats = monthly_stats.reset_index()
        
        print("\nMonthly Frame Scores by Outlet:")
        print(monthly_stats.to_string(index=False))
        
        return monthly_stats
    
    def show_example_articles(self, n: int = 5):
        """Show example articles with high frame scores for qualitative analysis."""
        
        print("\n" + "="*70)
        print("EXAMPLE ARTICLES (for qualitative illustration)")
        print("="*70)
        
        examples = {}
        
        # Most negative responsibility framing
        print("\n📌 MOST NEGATIVE RESPONSIBILITY FRAMING:")
        print("   (Articles assigning strongest blame)")
        neg_resp = self.df.nsmallest(n, 'resp_net_score')
        examples['neg_responsibility'] = neg_resp[['outlet', 'headline', 'resp_net_score', 'resp_neg_terms']].to_dict('records')
        for idx, row in neg_resp.iterrows():
            print(f"\n  [{row['outlet']}] \"{row['headline']}\"")
            print(f"    Score: {row['resp_net_score']:.6f}")
            print(f"    Blame terms: {row['resp_neg_terms']}")
        
        # Most positive responsibility framing
        print("\n\n📌 MOST POSITIVE RESPONSIBILITY FRAMING:")
        print("   (Articles giving strongest credit)")
        pos_resp = self.df.nlargest(n, 'resp_net_score')
        examples['pos_responsibility'] = pos_resp[['outlet', 'headline', 'resp_net_score', 'resp_pos_terms']].to_dict('records')
        for idx, row in pos_resp.iterrows():
            print(f"\n  [{row['outlet']}] \"{row['headline']}\"")
            print(f"    Score: {row['resp_net_score']:.6f}")
            print(f"    Credit terms: {row['resp_pos_terms']}")
        
        # Most negative science framing
        print("\n\n📌 MOST NEGATIVE SCIENCE FRAMING:")
        print("   (Articles portraying strongest anti-science stance)")
        neg_sci = self.df.nsmallest(n, 'sci_net_score')
        examples['neg_science'] = neg_sci[['outlet', 'headline', 'sci_net_score', 'sci_neg_terms']].to_dict('records')
        for idx, row in neg_sci.iterrows():
            print(f"\n  [{row['outlet']}] \"{row['headline']}\"")
            print(f"    Score: {row['sci_net_score']:.6f}")
            print(f"    Anti-science terms: {row['sci_neg_terms']}")
        
        # Most positive science framing
        print("\n\n📌 MOST POSITIVE SCIENCE FRAMING:")
        print("   (Articles portraying pro-science stance)")
        pos_sci = self.df.nlargest(n, 'sci_net_score')
        examples['pos_science'] = pos_sci[['outlet', 'headline', 'sci_net_score', 'sci_pos_terms']].to_dict('records')
        for idx, row in pos_sci.iterrows():
            print(f"\n  [{row['outlet']}] \"{row['headline']}\"")
            print(f"    Score: {row['sci_net_score']:.6f}")
            print(f"    Pro-science terms: {row['sci_pos_terms']}")
        
        print("\n" + "="*70)
        
        return examples
    
    def export_results(self, output_path: str):
        """Export results to CSV."""
        self.df.to_csv(output_path, index=False)
        print(f"\n✓ Results exported to: {output_path}")
    
    def generate_report(self, output_dir: str = None):
        """Generate a comprehensive analysis report."""
        
        if output_dir is None:
            output_dir = self.project_root / 'results'
        else:
            output_dir = Path(output_dir)
        
        output_dir.mkdir(exist_ok=True)
        
        # Export main results
        self.export_results(str(output_dir / 'framing_analysis_results.csv'))
        
        # Export summary statistics
        stats = self.generate_summary_statistics()
        pd.DataFrame(stats).T.to_csv(output_dir / 'summary_statistics.csv')
        
        # Export temporal analysis
        temporal = self.temporal_analysis()
        temporal.to_csv(output_dir / 'temporal_analysis.csv', index=False)
        
        print(f"\n✓ Report generated in: {output_dir}")


# ============================================================================
# PART 6: VISUALIZATION MODULE
# ============================================================================

class FramingVisualization:
    """
    Visualization module for framing analysis results.
    """
    
    def __init__(self, df: pd.DataFrame):
        self.df = df
        
    def plot_frame_distribution(self, save_path: str = None):
        """Plot distribution of frame directions by outlet."""
        import matplotlib.pyplot as plt
        import seaborn as sns
        
        fig, axes = plt.subplots(1, 2, figsize=(14, 5))
        
        # Responsibility frame
        resp_data = pd.crosstab(self.df['outlet'], self.df['resp_direction'], normalize='index') * 100
        resp_data = resp_data[['negative', 'neutral', 'positive', 'absent']]
        resp_data.plot(kind='bar', ax=axes[0], color=['#d62728', '#7f7f7f', '#2ca02c', '#9467bd'])
        axes[0].set_title('RQ1: Responsibility Frame Distribution', fontsize=12, fontweight='bold')
        axes[0].set_ylabel('Percentage of Articles')
        axes[0].set_xlabel('')
        axes[0].legend(title='Direction', loc='upper right')
        axes[0].tick_params(axis='x', rotation=0)
        
        # Science frame
        sci_data = pd.crosstab(self.df['outlet'], self.df['sci_direction'], normalize='index') * 100
        sci_data = sci_data[['negative', 'neutral', 'positive', 'absent']]
        sci_data.plot(kind='bar', ax=axes[1], color=['#d62728', '#7f7f7f', '#2ca02c', '#9467bd'])
        axes[1].set_title('RQ2: Science Frame Distribution', fontsize=12, fontweight='bold')
        axes[1].set_ylabel('Percentage of Articles')
        axes[1].set_xlabel('')
        axes[1].legend(title='Direction', loc='upper right')
        axes[1].tick_params(axis='x', rotation=0)
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            print(f"✓ Saved: {save_path}")
        
        plt.show()
    
    def plot_score_comparison(self, save_path: str = None):
        """Plot boxplot comparison of frame scores by outlet."""
        import matplotlib.pyplot as plt
        import seaborn as sns
        
        fig, axes = plt.subplots(1, 2, figsize=(12, 5))
        
        # Responsibility frame scores
        sns.boxplot(data=self.df, x='outlet', y='resp_net_score', ax=axes[0], 
                   palette={'NYT': '#1f77b4', 'Guardian': '#ff7f0e'})
        axes[0].axhline(y=0, color='gray', linestyle='--', alpha=0.5)
        axes[0].set_title('RQ1: Responsibility Frame Score Distribution', fontsize=12, fontweight='bold')
        axes[0].set_ylabel('Net Score (positive - negative) / words')
        axes[0].set_xlabel('')
        
        # Science frame scores
        sns.boxplot(data=self.df, x='outlet', y='sci_net_score', ax=axes[1],
                   palette={'NYT': '#1f77b4', 'Guardian': '#ff7f0e'})
        axes[1].axhline(y=0, color='gray', linestyle='--', alpha=0.5)
        axes[1].set_title('RQ2: Science Frame Score Distribution', fontsize=12, fontweight='bold')
        axes[1].set_ylabel('Net Score (positive - negative) / words')
        axes[1].set_xlabel('')
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            print(f"✓ Saved: {save_path}")
        
        plt.show()
    
    def plot_temporal_trends(self, save_path: str = None):
        """Plot temporal trends in framing."""
        import matplotlib.pyplot as plt
        
        # Monthly aggregation
        monthly = self.df.groupby(['month', 'outlet']).agg({
            'resp_net_score': 'mean',
            'sci_net_score': 'mean'
        }).reset_index()
        
        monthly['month'] = monthly['month'].astype(str)
        
        fig, axes = plt.subplots(2, 1, figsize=(12, 8))
        
        # Responsibility frame over time
        for outlet in ['NYT', 'Guardian']:
            outlet_data = monthly[monthly['outlet'] == outlet]
            axes[0].plot(outlet_data['month'], outlet_data['resp_net_score'], 
                        marker='o', label=outlet, linewidth=2)
        
        axes[0].axhline(y=0, color='gray', linestyle='--', alpha=0.5)
        axes[0].set_title('RQ1: Responsibility Frame Over Time', fontsize=12, fontweight='bold')
        axes[0].set_ylabel('Mean Net Score')
        axes[0].legend()
        axes[0].tick_params(axis='x', rotation=45)
        
        # Science frame over time
        for outlet in ['NYT', 'Guardian']:
            outlet_data = monthly[monthly['outlet'] == outlet]
            axes[1].plot(outlet_data['month'], outlet_data['sci_net_score'], 
                        marker='o', label=outlet, linewidth=2)
        
        axes[1].axhline(y=0, color='gray', linestyle='--', alpha=0.5)
        axes[1].set_title('RQ2: Science Frame Over Time', fontsize=12, fontweight='bold')
        axes[1].set_ylabel('Mean Net Score')
        axes[1].set_xlabel('Month')
        axes[1].legend()
        axes[1].tick_params(axis='x', rotation=45)
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            print(f"✓ Saved: {save_path}")
        
        plt.show()


# ============================================================================
# PART 7: MAIN EXECUTION
# ============================================================================

if __name__ == "__main__":
    """
    Run complete framing analysis pipeline.
    
    Usage:
        python two_dimension_framing_analysis.py
    
    Or import and use programmatically:
        from two_dimension_framing_analysis import FramingAnalysisPipeline
        pipeline = FramingAnalysisPipeline()
        pipeline.load_from_project_data()
        pipeline.analyze_frames()
        pipeline.print_summary()
    """
    
    print("="*70)
    print("TWO-DIMENSION FRAMING ANALYSIS")
    print("Trump COVID-19 Coverage: NYT vs. Guardian")
    print("Research Questions:")
    print("  RQ1: Responsibility attribution")
    print("  RQ2: Science/expertise framing")
    print("="*70)
    
    # Initialize pipeline
    pipeline = FramingAnalysisPipeline()
    
    # Load data from project directories
    try:
        pipeline.load_from_project_data()
    except Exception as e:
        print(f"\nError loading data: {e}")
        print("\nPlease ensure your data files are in:")
        print("  - nytimes_trump_covid/")
        print("  - guardian_trump_covid/")
        exit(1)
    
    # Run frame detection
    results_df = pipeline.analyze_frames()
    
    # Print summary statistics
    pipeline.print_summary()
    
    # Run statistical tests
    test_results = pipeline.run_statistical_tests()
    
    # Temporal analysis
    temporal = pipeline.temporal_analysis()
    
    # Show example articles
    examples = pipeline.show_example_articles(n=3)
    
    # Generate report
    pipeline.generate_report()
    
    # Optional: Visualizations (requires matplotlib/seaborn)
    try:
        viz = FramingVisualization(pipeline.df)
        
        results_dir = pipeline.project_root / 'results'
        results_dir.mkdir(exist_ok=True)
        
        viz.plot_frame_distribution(str(results_dir / 'frame_distribution.png'))
        viz.plot_score_comparison(str(results_dir / 'score_comparison.png'))
        viz.plot_temporal_trends(str(results_dir / 'temporal_trends.png'))
    except ImportError:
        print("\nNote: Install matplotlib and seaborn for visualizations:")
        print("  pip install matplotlib seaborn")
    
    print("\n✓ Analysis complete!")
