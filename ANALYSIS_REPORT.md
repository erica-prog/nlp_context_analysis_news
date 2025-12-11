# Framing Analysis Report: Trump COVID-19 Coverage
## NYT vs. The Guardian (January 2020 – January 2021)

---

## Executive Summary

This study examines how two elite, left-leaning English-language newspapers—**The New York Times (NYT)** and **The Guardian**—framed President Trump's handling of the COVID-19 pandemic. Using a lexicon-based framing analysis approach grounded in Entman's (1993) framing theory, we analyzed **8,807 articles** across two dimensions:

1. **Responsibility Frame** (RQ1): Who gets blamed or credited for COVID-19 outcomes?
2. **Science Frame** (RQ2): How is Trump's relationship with scientific expertise portrayed?

### Key Findings

| Finding | Summary |
|---------|---------|
| **Responsibility framing differs significantly** | NYT shows more negative responsibility framing than Guardian (p = 0.048*) |
| **Science framing similar** | No significant difference in science framing between outlets (p = 0.99) |
| **Negative framing dominates** | Both outlets frame Trump negatively on responsibility (-0.002 to -0.004 avg) and science (-0.001 avg) |
| **Low frame presence** | 84-95% of articles show "absent" framing—frame terms are sparse in headlines/abstracts |
| **Sample imbalance** | NYT (n=180) vs Guardian (n=8,627) limits comparative analysis |

---

## I. Research Questions

### RQ1: Responsibility Framing
> "How do NYT and The Guardian differ in attributing responsibility for COVID-19 outcomes during the Trump administration?"

**Operationalization:**
- **Positive (Credit)**: Leadership terms, achievement language, coordination praise
- **Negative (Blame)**: Failure terms, delay/warning language, accountability demands

### RQ2: Science Framing  
> "How do the two outlets frame Trump's relationship with scientific expertise during the pandemic?"

**Operationalization:**
- **Positive (Pro-Science)**: Expert consultation, evidence-based language, positive scientist mentions
- **Negative (Anti-Science)**: Science rejection, misinformation, expert conflicts, pandemic denial

---

## II. Methodology

### Data Collection

| Outlet | Articles | Date Range | Source |
|--------|----------|------------|--------|
| NYT | 180 | Jan 20, 2020 – Jan 19, 2021 | NYT API |
| Guardian | 8,627 | Jan 20, 2020 – Jan 19, 2021 | Guardian API |
| **Total** | **8,807** | Trump administration COVID period | |

**Filtering Criteria:**
- Articles pre-filtered during scraping for COVID + Trump content
- Text analyzed: Headline + Abstract/Snippet
- Date range: Trump administration COVID period (Jan 20, 2020 – Jan 20, 2021)

### Lexicon Development

| Frame | Positive Terms | Negative Terms | Total |
|-------|---------------|----------------|-------|
| Responsibility | 50 | 88 | 138 |
| Science | 50 | 99 | 149 |
| **Total** | **100** | **187** | **287** |

**Score Calculation:**
```
Net Score = (positive_matches - negative_matches) / total_words
```

This normalization allows comparison across articles of different lengths.

### Why Lexicon-Based Approach?

Unlike sentiment analysis tools like VADER, which measure emotional valence, our lexicon-based approach directly operationalizes:
- **Responsibility attribution** (who is causally responsible)
- **Science engagement** (credibility and expertise framing)

This captures *how* issues are characterized—distinct linguistic patterns that assign causality and credibility—rather than mere emotional tone.

---

## III. Results

### A. Descriptive Statistics

#### Overall Frame Distribution

| Metric | NYT (n=180) | Guardian (n=8,627) |
|--------|-------------|-------------------|
| **RESPONSIBILITY FRAME** |||
| Mean Net Score | -0.0037 | -0.0020 |
| Standard Deviation | 0.0147 | 0.0115 |
| Median | 0.0000 | 0.0000 |
| Average Intensity | 0.0065 | 0.0034 |
| % Negative (Blame) | 11.7% | 7.3% |
| % Positive (Credit) | 2.2% | 2.2% |
| % Neutral | 2.2% | 0.2% |
| % Absent | 83.9% | 90.2% |
| **SCIENCE FRAME** |||
| Mean Net Score | -0.0010 | -0.0010 |
| Standard Deviation | 0.0087 | 0.0075 |
| Median | 0.0000 | 0.0000 |
| Average Intensity | 0.0022 | 0.0015 |
| % Negative (Anti-Science) | 5.0% | 3.7% |
| % Positive (Pro-Science) | 2.2% | 0.8% |
| % Neutral | 0.0% | 0.1% |
| % Absent | 92.8% | 95.4% |

#### Interpretation

1. **Both outlets show negative net scores** for both frames, indicating more blame/anti-science language than credit/pro-science language on average.

2. **Frame absence is high** (84-95%), suggesting that most article headlines and abstracts don't contain explicit frame terminology. This is expected since we only analyzed headlines + abstracts, not full article text.

3. **NYT shows more negative responsibility framing** (11.7% vs 7.3% negative), and this difference is statistically significant (p = 0.048).

4. **Science framing is nearly identical** between outlets with no significant difference.

---

### B. Statistical Tests

#### Responsibility Frame Comparison

| Test | Statistic | p-value | Interpretation |
|------|-----------|---------|----------------|
| Independent t-test | t = -1.974 | p = 0.0484* | Significant |
| Mann-Whitney U | U = 742,787 | p = 0.0520 | Marginally significant |
| Chi-square (direction) | χ² = 30.86, df = 3 | p < 0.0001*** | Highly significant |
| **Cohen's d** | **-0.149** | — | **Negligible effect** |
| Cramér's V | 0.059 | — | Small association |

#### Science Frame Comparison

| Test | Statistic | p-value | Interpretation |
|------|-----------|---------|----------------|
| Independent t-test | t = -0.013 | p = 0.9897 | Not significant |
| Mann-Whitney U | U = 776,442 | p = 0.9992 | Not significant |
| Chi-square (direction) | χ² = 4.95, df = 3 | p = 0.1758 | Not significant |
| **Cohen's d** | **-0.001** | — | **Negligible effect** |
| Cramér's V | 0.024 | — | Negligible association |

#### Summary
> **RQ1 (Responsibility):** Statistically significant difference found (p = 0.048*). NYT assigns more blame than Guardian, though effect size is negligible (Cohen's d = -0.149).
> 
> **RQ2 (Science):** No statistically significant difference found (p = 0.99). Both outlets frame Trump's relationship with science virtually identically.

---

### C. Temporal Analysis

#### Monthly Trends in Responsibility Framing

| Month | Guardian (n) | Guardian Mean | Guardian Blame % | NYT (n) | NYT Mean | NYT Blame % |
|-------|-------------|---------------|------------------|---------|----------|-------------|
| Jan 2020 | 139 | -0.0020 | 5.8% | 4 | 0.0000 | 0.0% |
| Feb 2020 | 342 | -0.0027 | 7.6% | 14 | 0.0000 | 0.0% |
| Mar 2020 | 903 | -0.0025 | 8.3% | 25 | -0.0047 | 12.0% |
| **Apr 2020** | **917** | **-0.0025** | **8.7%** | **15** | **-0.0123** | **26.7%** |
| **May 2020** | 882 | -0.0024 | 9.3% | 23 | -0.0045 | 17.4% |
| Jun 2020 | 691 | -0.0019 | 7.2% | 14 | -0.0043 | 7.1% |
| Jul 2020 | 670 | -0.0023 | 7.5% | 19 | -0.0016 | 5.3% |
| **Aug 2020** | 661 | -0.0023 | 8.6% | **19** | -0.0013 | **15.8%** |
| Sep 2020 | 659 | -0.0018 | 7.0% | 20 | -0.0055 | 15.0% |
| Oct 2020 | 873 | -0.0009 | 5.5% | 18 | -0.0037 | 11.1% |
| Nov 2020 | 905 | -0.0007 | 5.0% | 4 | 0.0000 | 0.0% |
| Dec 2020 | 543 | -0.0017 | 5.5% | 3 | +0.0098 | 0.0% |
| Jan 2021 | 442 | -0.0031 | 8.1% | 2 | 0.0000 | 0.0% |

#### Key Temporal Observations

1. **April 2020 peak in NYT**: NYT showed 26.7% blame framing in April 2020—the highest for any month—during the initial surge crisis

2. **Consistent Guardian coverage**: Guardian maintained 5-9% blame framing throughout, with more stable patterns

3. **NYT spikes in crisis moments**: NYT blame framing peaked during critical pandemic moments (April, August, September 2020)

4. **Article volume disparity**: Guardian published ~48x more articles than NYT in this period, limiting month-by-month NYT comparisons

5. **October-November decline**: Both outlets showed reduced blame framing during the election period

---

### D. Qualitative Examples

#### Most Negative Responsibility Framing

1. **Guardian**: "Corrections and clarifications"
   - Score: -0.286
   - Blame terms: "toll", "death toll"

2. **Guardian**: "Corrections and clarifications"
   - Score: -0.222
   - Blame terms: "toll", "death toll"

3. **Guardian**: "Trump campaign jubilant as Democrats' big night implodes"
   - Score: -0.105
   - Blame terms: "chaos", "disaster"

#### Most Positive Responsibility Framing

1. **Guardian**: "Trump unveils 'warp-speed' effort to create coronavirus vaccine by year's end"
   - Score: +0.111
   - Credit terms: "operation warp speed", "vaccine development", "warp speed", "rapid vaccine"

2. **Guardian**: "Biden team plan shake-up to get coronavirus shots into US arms"
   - Score: +0.065
   - Credit terms: "operation warp speed", "warp speed", "record time"

3. **Guardian**: "Manufacturers ask government to step in to limit coronavirus damage"
   - Score: +0.057
   - Credit terms: "decisive action", "decisive"

#### Most Negative Science Framing

1. **Guardian**: "The biggest Coalition conspiracy theory is climate change denial"
   - Score: -0.111
   - Anti-science terms: "unfounded claims", "misinformation", "conspiracy theory"

2. **Guardian**: "Twitter limits Donald Trump Jr's account for posting Covid-19 misinformation"
   - Score: -0.111
   - Anti-science terms: "misinformation", "hydroxychloroquine", "false claims"

3. **Guardian**: "NHS announces plan to combat coronavirus fake news"
   - Score: -0.111
   - Anti-science terms: "fake news", "disinformation"

#### Most Positive Science Framing

1. **Guardian**: "CDC director takes aim at Trump's Covid adviser: 'Everything he says is false'"
   - Score: +0.069
   - Pro-science terms: "anthony fauci", "cdc director"

2. **Guardian**: "Trump unveils 'warp-speed' effort to create coronavirus vaccine by year's end"
   - Score: +0.056
   - Pro-science terms: "operation warp speed", "vaccine development"

3. **Guardian**: "These self-appointed coronavirus experts really need to pipe down"
   - Score: +0.042
   - Pro-science terms: "epidemiologists"

---

## IV. Discussion

### Key Findings Summary

1. **Significant Responsibility Framing Difference**: Unlike previous analysis with smaller samples, this larger dataset reveals a statistically significant difference in responsibility framing (p = 0.048). NYT assigns more blame to Trump (11.7% negative) than Guardian (7.3% negative).

2. **Convergent Science Framing**: Both outlets showed remarkably similar science framing patterns (p = 0.99), suggesting a shared perspective on Trump's relationship with scientific expertise.

3. **Negative Framing Predominates**: Both outlets predominantly used negative framing when frame terms were present:
   - NYT: 11.7% blame vs 2.2% credit (5.3:1 ratio)
   - Guardian: 7.3% blame vs 2.2% credit (3.3:1 ratio)

4. **Sparse Frame Presence**: The majority of articles (84-95%) contained no detectable frame terms in headlines/abstracts. This highlights a limitation of analyzing only headlines rather than full article text.

5. **"Operation Warp Speed" as Positive Exception**: The vaccine development effort received positive framing in both frames—representing Trump's most favorable coverage.

### Implications for Research Questions

#### RQ1: Responsibility Attribution
NYT shows significantly more negative responsibility framing than Guardian. This suggests that despite both being left-leaning outlets, the US-based NYT held Trump to a stricter standard of accountability for COVID outcomes than the UK-based Guardian. However, the effect size is negligible, indicating the practical difference is small.

#### RQ2: Science/Expertise Framing
Both outlets framed Trump's relationship with science virtually identically. This suggests a transnational consensus on portraying Trump as anti-science, with similar emphasis on conflicts with Fauci, misinformation promotion, and hydroxychloroquine advocacy.

### Theoretical Implications

These findings partially support the **indexing hypothesis** (Bennett, 1990): while science framing converged across outlets, responsibility framing showed national differences. The US press may have stronger norms around presidential accountability.

The **negativity bias** in political coverage (Patterson, 2016) is evident—negative outcomes received 3-5x more frame-laden coverage than positive developments.

---

## V. Limitations

### 1. Sample Size Imbalance
- **NYT: 180 articles** vs **Guardian: 8,627 articles**
- This 48:1 ratio limits statistical power for comparative analysis
- NYT's smaller sample reflects API pagination limits (max 1,000 results per query)

### 2. Text Scope
- Analysis limited to **headlines + abstracts only**
- Full article text would likely reveal more frame terms
- Headlines may be crafted for attention rather than frame expression

### 3. Lexicon Limitations
- Pre-defined lexicons may miss emergent frame language
- Some terms are context-dependent (e.g., "under control" could be descriptive or sarcastic)
- Multi-word phrases may not capture all relevant framing

### 4. Temporal Coverage
- Only covers Trump administration period (Jan 2020 - Jan 2021)
- Cannot assess how framing changed post-Trump or during Biden administration

### 5. Limited Outlet Comparison
- Only two outlets limits generalizability
- Both are elite, left-leaning publications
- Conservative outlets (Fox News, Wall Street Journal) would likely show different patterns

---

## VI. Conclusions

This lexicon-based framing analysis of 8,807 articles reveals:

1. **Statistically significant difference in responsibility framing** (p = 0.048): NYT assigns more blame to Trump than Guardian, though effect size is negligible

2. **No significant difference in science framing** (p = 0.99): Both outlets portrayed Trump's relationship with science virtually identically

3. **Both outlets used predominantly negative framing**:
   - Responsibility: 7-12% blame vs 2% credit
   - Science: 4-5% anti-science vs 1-2% pro-science

4. **"Operation Warp Speed"** represented the most consistent positive framing across both outlets

5. **Frame absence is high** (84-95%), suggesting explicit frame terms are sparse in headlines/abstracts

Future research should:
- Scrape more NYT articles to balance the dataset
- Include conservative media outlets for comparison
- Analyze full article text rather than just headlines
- Employ manual validation of a subset of articles

---

## VII. Network Visualization Analysis

### Analysis Results

Analyzed **8,807 articles** from Trump COVID-19 coverage (NYT vs. Guardian)

#### Top Frame Terms Found:

| Category | Top Terms |
|----------|-----------|
| **Responsibility+ (Credit)** | delivered (94), leadership (66), successful (19) |
| **Responsibility- (Blame)** | toll (260), death toll (219), chaos (79), failed (72) |
| **Science+ (Pro-Science)** | anthony fauci (30), dr fauci (10), deborah birx (10) |
| **Science- (Anti-Science)** | hydroxychloroquine (66), misinformation (60), baseless (33) |

#### Network Statistics:

- **33 nodes** (unique frame terms)
- **46 edges** (co-occurrence connections)
- **Network density:** 0.0871
- **Most central terms:** toll, death toll, failure, misinformation, leadership



#### Outlet Comparison Word Clouds

The `word_clouds_by_outlet.png` visualization provides a direct comparison of frame term usage between NYT and Guardian. Each row represents an outlet, with columns showing the four frame categories (Responsibility+/-, Science+/-). This allows visual identification of:
- Which blame/credit terms each outlet emphasizes
- Differences in science-related terminology between US and UK coverage
- Relative term diversity (more varied word sizes indicate more distributed term usage)

#### Generated Visualizations (saved to `results/`):

| File | Description |
|------|-------------|
| `word_clouds.png` | Word clouds for each frame category (combined) |
| `word_clouds_by_outlet.png` | Side-by-side word clouds comparing NYT vs Guardian framing |
| `top_terms_frequency.png` | Bar charts of top terms |
| `cross_frame_network.png` | Bipartite network showing responsibility ↔ science connections |
| `full_frame_network.png` | Complete network of all term co-occurrences |
---

## VIII. Appendix: Statistical Output

### Full Statistical Test Results

```
RESPONSIBILITY FRAME:
  NYT mean: -0.003719 (SD: 0.014656)
  Guardian mean: -0.001991 (SD: 0.011547)
  
  t-test: t = -1.974, p = 0.0484 *
  Mann-Whitney U: U = 742787, p = 0.0520
  Cohen's d: -0.149 (negligible effect)
  Chi-square: χ² = 30.86, df = 3, p < 0.0001 ***
  Cramér's V: 0.059

SCIENCE FRAME:
  NYT mean: -0.001013 (SD: 0.008682)
  Guardian mean: -0.001006 (SD: 0.007537)
  
  t-test: t = -0.013, p = 0.9897
  Mann-Whitney U: U = 776442, p = 0.9992
  Cohen's d: -0.001 (negligible effect)
  Chi-square: χ² = 4.95, df = 3, p = 0.1758
  Cramér's V: 0.024

Note: * p<0.05, ** p<0.01, *** p<0.001
```

### Lexicon Term Counts

| Category | N Terms |
|----------|---------|
| Responsibility Positive | 50 |
| Responsibility Negative | 88 |
| Science Positive | 50 |
| Science Negative | 99 |
| **Total** | **287** |

---

## IX. Output Files

| File | Description |
|------|-------------|
| `results/framing_analysis_results.csv` | Full dataset with frame scores |
| `results/summary_statistics.csv` | Summary statistics by outlet |
| `results/temporal_analysis.csv` | Monthly frame trends |
| `results/frame_distribution.png` | Frame direction distribution chart |
| `results/score_comparison.png` | Boxplot score comparison |
| `results/temporal_trends.png` | Timeline visualization |

---

## X. References

Bennett, W. L. (1990). Toward a theory of press-state relations in the United States. *Journal of Communication*, 40(2), 103-127.

Entman, R. M. (1993). Framing: Toward clarification of a fractured paradigm. *Journal of Communication*, 43(4), 51-58.

Patterson, T. E. (2016). Pre-primary news coverage of the 2016 presidential race: Trump's rise, Sanders' emergence, Clinton's struggle. *Shorenstein Center on Media, Politics and Public Policy*.

---

*Report updated: December 2025*  
*Analysis script: `src/two_dimension_framing_analysis.py`*  
*Total articles analyzed: 8,807 (NYT: 180, Guardian: 8,627)*
