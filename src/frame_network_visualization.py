"""
Frame Network Visualization & Word Cloud Analysis
Trump COVID-19 Coverage: NYT vs. Guardian

Visualizes:
1. Word clouds for each frame dimension
2. Network graph of term co-occurrences within articles
3. Cross-frame connections (responsibility ↔ science terms)
4. Top words frequency analysis
"""

import pandas as pd
import numpy as np
from pathlib import Path
from collections import defaultdict, Counter
from typing import Dict, List, Tuple, Set
import re
import warnings

warnings.filterwarnings('ignore')

# Import frame lexicons from main analysis
from two_dimension_framing_analysis import FrameLexicons, TwoDimensionFrameDetector


# ============================================================================
# PART 1: TEXT ANALYSIS & TERM EXTRACTION
# ============================================================================

class FrameTermAnalyzer:
    """
    Analyzes term frequencies and co-occurrences across articles.
    """
    
    def __init__(self):
        self.lexicons = FrameLexicons()
        self.detector = TwoDimensionFrameDetector()
        
        # Flatten all lexicon terms
        self.resp_pos = self.lexicons.RESPONSIBILITY_POSITIVE
        self.resp_neg = self.lexicons.RESPONSIBILITY_NEGATIVE
        self.sci_pos = self.lexicons.SCIENCE_POSITIVE
        self.sci_neg = self.lexicons.SCIENCE_NEGATIVE
        
        self.all_resp_terms = self.resp_pos | self.resp_neg
        self.all_sci_terms = self.sci_pos | self.sci_neg
        self.all_terms = self.all_resp_terms | self.all_sci_terms
        
    def extract_terms_from_text(self, text: str) -> Dict[str, List[str]]:
        """
        Extract all frame terms found in a single article.
        Returns categorized lists of terms.
        """
        text = self.detector.preprocess_text(text)
        
        found = {
            'resp_pos': [],
            'resp_neg': [],
            'sci_pos': [],
            'sci_neg': []
        }
        
        for term in self.resp_pos:
            if re.search(r'\b' + re.escape(term.lower()) + r'\b', text):
                found['resp_pos'].append(term)
                
        for term in self.resp_neg:
            if re.search(r'\b' + re.escape(term.lower()) + r'\b', text):
                found['resp_neg'].append(term)
                
        for term in self.sci_pos:
            if re.search(r'\b' + re.escape(term.lower()) + r'\b', text):
                found['sci_pos'].append(term)
                
        for term in self.sci_neg:
            if re.search(r'\b' + re.escape(term.lower()) + r'\b', text):
                found['sci_neg'].append(term)
        
        return found
    
    def analyze_corpus(self, df: pd.DataFrame) -> Dict:
        """
        Analyze entire corpus for term frequencies and co-occurrences.
        """
        # Term frequency counters
        term_freq = {
            'resp_pos': Counter(),
            'resp_neg': Counter(),
            'sci_pos': Counter(),
            'sci_neg': Counter()
        }
        
        # Co-occurrence matrices (term pairs appearing in same article)
        cooccur_within_resp = Counter()  # responsibility term pairs
        cooccur_within_sci = Counter()   # science term pairs
        cooccur_cross_frame = Counter()  # responsibility-science pairs
        
        # Track which articles have which terms
        articles_with_terms = defaultdict(list)
        
        print("Analyzing term frequencies and co-occurrences...")
        
        for idx, row in df.iterrows():
            if idx % 500 == 0:
                print(f"  Processing article {idx}/{len(df)}...")
            
            text = row['text']
            terms = self.extract_terms_from_text(text)
            
            # Count frequencies
            for category, term_list in terms.items():
                for term in term_list:
                    term_freq[category][term] += 1
                    articles_with_terms[term].append(idx)
            
            # Co-occurrences within responsibility frame
            all_resp = terms['resp_pos'] + terms['resp_neg']
            for i, t1 in enumerate(all_resp):
                for t2 in all_resp[i+1:]:
                    pair = tuple(sorted([t1, t2]))
                    cooccur_within_resp[pair] += 1
            
            # Co-occurrences within science frame
            all_sci = terms['sci_pos'] + terms['sci_neg']
            for i, t1 in enumerate(all_sci):
                for t2 in all_sci[i+1:]:
                    pair = tuple(sorted([t1, t2]))
                    cooccur_within_sci[pair] += 1
            
            # Cross-frame co-occurrences
            for resp_term in all_resp:
                for sci_term in all_sci:
                    cooccur_cross_frame[(resp_term, sci_term)] += 1
        
        print("  ✓ Analysis complete!")
        
        return {
            'term_freq': term_freq,
            'cooccur_within_resp': cooccur_within_resp,
            'cooccur_within_sci': cooccur_within_sci,
            'cooccur_cross_frame': cooccur_cross_frame,
            'articles_with_terms': dict(articles_with_terms)
        }
    
    def get_top_terms(self, term_freq: Dict, n: int = 20) -> Dict:
        """Get top N terms for each category."""
        return {
            category: counter.most_common(n)
            for category, counter in term_freq.items()
        }
    
    def analyze_corpus_by_outlet(self, df: pd.DataFrame) -> Dict[str, Dict]:
        """
        Analyze term frequencies separately for each outlet (NYT vs Guardian).
        
        Returns:
            Dictionary with outlet names as keys, each containing term_freq dict
        """
        results = {}
        
        for outlet in df['outlet'].unique():
            outlet_df = df[df['outlet'] == outlet]
            print(f"\nAnalyzing {outlet} ({len(outlet_df)} articles)...")
            
            term_freq = {
                'resp_pos': Counter(),
                'resp_neg': Counter(),
                'sci_pos': Counter(),
                'sci_neg': Counter()
            }
            
            for idx, row in outlet_df.iterrows():
                text = row['text']
                terms = self.extract_terms_from_text(text)
                
                for category, term_list in terms.items():
                    for term in term_list:
                        term_freq[category][term] += 1
            
            results[outlet] = {'term_freq': term_freq}
            print(f"  ✓ {outlet} analysis complete!")
        
        return results


# ============================================================================
# PART 2: NETWORK GRAPH BUILDER
# ============================================================================

class FrameNetworkBuilder:
    """
    Builds network graphs from term co-occurrence data.
    """
    
    def __init__(self):
        self.lexicons = FrameLexicons()
        
    def build_cooccurrence_network(self, cooccur_data: Counter, 
                                   min_weight: int = 2) -> 'networkx.Graph':
        """
        Build a network graph from co-occurrence data.
        
        Args:
            cooccur_data: Counter of (term1, term2) -> count
            min_weight: Minimum co-occurrence count to include edge
        """
        import networkx as nx
        
        G = nx.Graph()
        
        for (t1, t2), weight in cooccur_data.items():
            if weight >= min_weight:
                G.add_edge(t1, t2, weight=weight)
        
        return G
    
    def build_cross_frame_network(self, cooccur_cross: Counter,
                                  min_weight: int = 2) -> 'networkx.Graph':
        """
        Build bipartite-style network between responsibility and science terms.
        """
        import networkx as nx
        
        G = nx.Graph()
        
        resp_terms = set()
        sci_terms = set()
        
        for (resp_term, sci_term), weight in cooccur_cross.items():
            if weight >= min_weight:
                G.add_edge(resp_term, sci_term, weight=weight)
                resp_terms.add(resp_term)
                sci_terms.add(sci_term)
        
        # Add node attributes for frame type
        for node in G.nodes():
            if node in self.lexicons.RESPONSIBILITY_POSITIVE:
                G.nodes[node]['frame'] = 'responsibility'
                G.nodes[node]['valence'] = 'positive'
                G.nodes[node]['color'] = '#2ecc71'  # Green
            elif node in self.lexicons.RESPONSIBILITY_NEGATIVE:
                G.nodes[node]['frame'] = 'responsibility'
                G.nodes[node]['valence'] = 'negative'
                G.nodes[node]['color'] = '#e74c3c'  # Red
            elif node in self.lexicons.SCIENCE_POSITIVE:
                G.nodes[node]['frame'] = 'science'
                G.nodes[node]['valence'] = 'positive'
                G.nodes[node]['color'] = '#3498db'  # Blue
            elif node in self.lexicons.SCIENCE_NEGATIVE:
                G.nodes[node]['frame'] = 'science'
                G.nodes[node]['valence'] = 'negative'
                G.nodes[node]['color'] = '#9b59b6'  # Purple
        
        return G
    
    def build_full_frame_network(self, analysis_results: Dict,
                                 min_within: int = 2,
                                 min_cross: int = 2) -> 'networkx.Graph':
        """
        Build complete network with both within-frame and cross-frame connections.
        """
        import networkx as nx
        
        G = nx.Graph()
        
        # Add within-frame edges
        for (t1, t2), weight in analysis_results['cooccur_within_resp'].items():
            if weight >= min_within:
                G.add_edge(t1, t2, weight=weight, edge_type='within_resp')
        
        for (t1, t2), weight in analysis_results['cooccur_within_sci'].items():
            if weight >= min_within:
                G.add_edge(t1, t2, weight=weight, edge_type='within_sci')
        
        # Add cross-frame edges
        for (resp_term, sci_term), weight in analysis_results['cooccur_cross_frame'].items():
            if weight >= min_cross:
                if G.has_edge(resp_term, sci_term):
                    G[resp_term][sci_term]['weight'] += weight
                else:
                    G.add_edge(resp_term, sci_term, weight=weight, edge_type='cross_frame')
        
        # Add node attributes
        for node in G.nodes():
            if node in self.lexicons.RESPONSIBILITY_POSITIVE:
                G.nodes[node]['frame'] = 'responsibility'
                G.nodes[node]['valence'] = 'positive'
            elif node in self.lexicons.RESPONSIBILITY_NEGATIVE:
                G.nodes[node]['frame'] = 'responsibility'
                G.nodes[node]['valence'] = 'negative'
            elif node in self.lexicons.SCIENCE_POSITIVE:
                G.nodes[node]['frame'] = 'science'
                G.nodes[node]['valence'] = 'positive'
            elif node in self.lexicons.SCIENCE_NEGATIVE:
                G.nodes[node]['frame'] = 'science'
                G.nodes[node]['valence'] = 'negative'
        
        return G


# ============================================================================
# PART 3: VISUALIZATION
# ============================================================================

class FrameVisualization:
    """
    Creates visualizations: word clouds, network graphs, frequency charts.
    """
    
    def __init__(self, output_dir: str = None):
        self.output_dir = Path(output_dir) if output_dir else Path('results')
        self.output_dir.mkdir(exist_ok=True)
        
    def plot_word_clouds(self, term_freq: Dict, save: bool = True):
        """
        Create word clouds for each frame category.
        """
        import matplotlib.pyplot as plt
        from wordcloud import WordCloud
        
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        
        categories = [
            ('resp_pos', 'Responsibility: Positive (Credit)', '#27ae60', axes[0, 0]),
            ('resp_neg', 'Responsibility: Negative (Blame)', '#c0392b', axes[0, 1]),
            ('sci_pos', 'Science: Positive (Pro-Science)', '#2980b9', axes[1, 0]),
            ('sci_neg', 'Science: Negative (Anti-Science)', '#8e44ad', axes[1, 1])
        ]
        
        for category, title, color, ax in categories:
            freq_dict = dict(term_freq[category])
            
            if freq_dict:
                wc = WordCloud(
                    width=800, height=400,
                    background_color='white',
                    color_func=lambda *args, **kwargs: color,
                    max_words=50,
                    min_font_size=10,
                    prefer_horizontal=0.7
                ).generate_from_frequencies(freq_dict)
                
                ax.imshow(wc, interpolation='bilinear')
                ax.set_title(title, fontsize=14, fontweight='bold', pad=10)
            else:
                ax.text(0.5, 0.5, 'No terms found', ha='center', va='center', fontsize=12)
                ax.set_title(title, fontsize=14, fontweight='bold', pad=10)
            
            ax.axis('off')
        
        plt.suptitle('Frame Term Word Clouds\nTrump COVID-19 Coverage', 
                    fontsize=16, fontweight='bold', y=1.02)
        plt.tight_layout()
        
        if save:
            path = self.output_dir / 'word_clouds.png'
            plt.savefig(path, dpi=300, bbox_inches='tight', facecolor='white')
            print(f"✓ Saved: {path}")
        
        plt.show()
    
    def plot_word_clouds_by_outlet(self, outlet_results: Dict[str, Dict], save: bool = True):
        """
        Create word clouds for each outlet separately.
        
        Args:
            outlet_results: Dictionary with outlet names as keys, containing term_freq
        """
        import matplotlib.pyplot as plt
        from wordcloud import WordCloud
        
        # Define colors for each category
        category_colors = {
            'resp_pos': '#27ae60',  # Green - Credit
            'resp_neg': '#c0392b',  # Red - Blame
            'sci_pos': '#2980b9',   # Blue - Pro-Science
            'sci_neg': '#8e44ad'    # Purple - Anti-Science
        }
        
        category_titles = {
            'resp_pos': 'Responsibility+\n(Credit)',
            'resp_neg': 'Responsibility-\n(Blame)',
            'sci_pos': 'Science+\n(Pro-Science)',
            'sci_neg': 'Science-\n(Anti-Science)'
        }
        
        outlets = list(outlet_results.keys())
        n_outlets = len(outlets)
        
        # Create figure: rows = outlets, cols = 4 categories
        fig, axes = plt.subplots(n_outlets, 4, figsize=(20, 6 * n_outlets))
        
        # Handle single outlet case
        if n_outlets == 1:
            axes = axes.reshape(1, -1)
        
        for row_idx, outlet in enumerate(outlets):
            term_freq = outlet_results[outlet]['term_freq']
            
            for col_idx, (category, color) in enumerate(category_colors.items()):
                ax = axes[row_idx, col_idx]
                freq_dict = dict(term_freq[category])
                
                if freq_dict:
                    # Create color function with closure to capture current color
                    def make_color_func(c):
                        return lambda *args, **kwargs: c
                    
                    wc = WordCloud(
                        width=600, height=400,
                        background_color='white',
                        color_func=make_color_func(color),
                        max_words=40,
                        min_font_size=8,
                        prefer_horizontal=0.7
                    ).generate_from_frequencies(freq_dict)
                    
                    ax.imshow(wc, interpolation='bilinear')
                else:
                    ax.text(0.5, 0.5, 'No terms found', ha='center', va='center', 
                           fontsize=12, color='gray')
                
                ax.axis('off')
                
                # Add column titles on first row
                if row_idx == 0:
                    ax.set_title(category_titles[category], fontsize=12, fontweight='bold', pad=10)
            
            # Add outlet label on left side
            axes[row_idx, 0].text(-0.15, 0.5, outlet.upper(), transform=axes[row_idx, 0].transAxes,
                                  fontsize=16, fontweight='bold', va='center', ha='right',
                                  rotation=90)
        
        plt.suptitle('Frame Term Word Clouds by Outlet\nTrump COVID-19 Coverage: NYT vs. Guardian', 
                    fontsize=18, fontweight='bold', y=0.98)
        plt.tight_layout()
        
        if save:
            path = self.output_dir / 'word_clouds_by_outlet.png'
            plt.savefig(path, dpi=300, bbox_inches='tight', facecolor='white')
            print(f"✓ Saved: {path}")
        
        plt.show()
    
    def plot_top_terms_bar(self, term_freq: Dict, n: int = 15, save: bool = True):
        """
        Create bar charts of top terms for each category.
        """
        import matplotlib.pyplot as plt
        
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        
        categories = [
            ('resp_pos', 'Responsibility: Credit Terms', '#27ae60', axes[0, 0]),
            ('resp_neg', 'Responsibility: Blame Terms', '#c0392b', axes[0, 1]),
            ('sci_pos', 'Science: Pro-Science Terms', '#2980b9', axes[1, 0]),
            ('sci_neg', 'Science: Anti-Science Terms', '#8e44ad', axes[1, 1])
        ]
        
        for category, title, color, ax in categories:
            top_terms = term_freq[category].most_common(n)
            
            if top_terms:
                terms, counts = zip(*top_terms)
                y_pos = range(len(terms))
                
                ax.barh(y_pos, counts, color=color, alpha=0.8)
                ax.set_yticks(y_pos)
                ax.set_yticklabels(terms, fontsize=10)
                ax.invert_yaxis()
                ax.set_xlabel('Frequency', fontsize=11)
                ax.set_title(title, fontsize=12, fontweight='bold')
                
                # Add count labels
                for i, (term, count) in enumerate(top_terms):
                    ax.text(count + 0.5, i, str(count), va='center', fontsize=9)
            else:
                ax.text(0.5, 0.5, 'No terms found', ha='center', va='center')
                ax.set_title(title, fontsize=12, fontweight='bold')
        
        plt.suptitle('Top Frame Terms by Frequency\nTrump COVID-19 Coverage',
                    fontsize=14, fontweight='bold', y=1.02)
        plt.tight_layout()
        
        if save:
            path = self.output_dir / 'top_terms_frequency.png'
            plt.savefig(path, dpi=300, bbox_inches='tight', facecolor='white')
            print(f"✓ Saved: {path}")
        
        plt.show()
    
    def plot_network_graph(self, G: 'networkx.Graph', title: str,
                          layout: str = 'spring', save_name: str = None):
        """
        Visualize a network graph with customized styling.
        """
        import matplotlib.pyplot as plt
        import networkx as nx
        
        if len(G.nodes()) == 0:
            print("Warning: Network has no nodes to display")
            return
        
        fig, ax = plt.subplots(1, 1, figsize=(16, 12))
        
        # Choose layout
        if layout == 'spring':
            pos = nx.spring_layout(G, k=2, iterations=50, seed=42)
        elif layout == 'kamada_kawai':
            pos = nx.kamada_kawai_layout(G)
        elif layout == 'circular':
            pos = nx.circular_layout(G)
        else:
            pos = nx.spring_layout(G, k=2, seed=42)
        
        # Get node colors based on frame and valence
        node_colors = []
        for node in G.nodes():
            frame = G.nodes[node].get('frame', 'unknown')
            valence = G.nodes[node].get('valence', 'unknown')
            
            if frame == 'responsibility' and valence == 'positive':
                node_colors.append('#2ecc71')  # Green
            elif frame == 'responsibility' and valence == 'negative':
                node_colors.append('#e74c3c')  # Red
            elif frame == 'science' and valence == 'positive':
                node_colors.append('#3498db')  # Blue
            elif frame == 'science' and valence == 'negative':
                node_colors.append('#9b59b6')  # Purple
            else:
                node_colors.append('#95a5a6')  # Gray
        
        # Get edge weights for width
        edge_weights = [G[u][v].get('weight', 1) for u, v in G.edges()]
        max_weight = max(edge_weights) if edge_weights else 1
        edge_widths = [1 + 3 * (w / max_weight) for w in edge_weights]
        
        # Get node degrees for size
        node_sizes = [300 + 100 * G.degree(node) for node in G.nodes()]
        
        # Draw network
        nx.draw_networkx_edges(G, pos, width=edge_widths, alpha=0.4, 
                              edge_color='gray', ax=ax)
        nx.draw_networkx_nodes(G, pos, node_color=node_colors, 
                              node_size=node_sizes, alpha=0.9, ax=ax)
        nx.draw_networkx_labels(G, pos, font_size=8, font_weight='bold', ax=ax)
        
        # Add legend
        from matplotlib.patches import Patch
        legend_elements = [
            Patch(facecolor='#2ecc71', label='Responsibility +'),
            Patch(facecolor='#e74c3c', label='Responsibility -'),
            Patch(facecolor='#3498db', label='Science +'),
            Patch(facecolor='#9b59b6', label='Science -'),
        ]
        ax.legend(handles=legend_elements, loc='upper left', fontsize=10)
        
        ax.set_title(title, fontsize=14, fontweight='bold')
        ax.axis('off')
        
        plt.tight_layout()
        
        if save_name:
            path = self.output_dir / f'{save_name}.png'
            plt.savefig(path, dpi=300, bbox_inches='tight', facecolor='white')
            print(f"✓ Saved: {path}")
        
        plt.show()
    
    def plot_cross_frame_network(self, cooccur_cross: Counter, 
                                 min_weight: int = 2, save: bool = True):
        """
        Visualize cross-frame connections as a bipartite-style network.
        """
        import matplotlib.pyplot as plt
        import networkx as nx
        
        # Build network
        builder = FrameNetworkBuilder()
        G = builder.build_cross_frame_network(cooccur_cross, min_weight=min_weight)
        
        if len(G.nodes()) == 0:
            print("Warning: No cross-frame connections above threshold")
            return
        
        fig, ax = plt.subplots(1, 1, figsize=(18, 14))
        
        # Separate nodes by frame
        resp_nodes = [n for n in G.nodes() if G.nodes[n].get('frame') == 'responsibility']
        sci_nodes = [n for n in G.nodes() if G.nodes[n].get('frame') == 'science']
        
        # Create bipartite-style layout
        pos = {}
        
        # Responsibility terms on left
        for i, node in enumerate(sorted(resp_nodes)):
            pos[node] = (-1, i - len(resp_nodes)/2)
        
        # Science terms on right
        for i, node in enumerate(sorted(sci_nodes)):
            pos[node] = (1, i - len(sci_nodes)/2)
        
        # Get colors
        node_colors = []
        for node in G.nodes():
            valence = G.nodes[node].get('valence', 'unknown')
            frame = G.nodes[node].get('frame', 'unknown')
            
            if frame == 'responsibility' and valence == 'positive':
                node_colors.append('#2ecc71')
            elif frame == 'responsibility' and valence == 'negative':
                node_colors.append('#e74c3c')
            elif frame == 'science' and valence == 'positive':
                node_colors.append('#3498db')
            elif frame == 'science' and valence == 'negative':
                node_colors.append('#9b59b6')
            else:
                node_colors.append('#95a5a6')
        
        # Edge weights
        edge_weights = [G[u][v].get('weight', 1) for u, v in G.edges()]
        max_weight = max(edge_weights) if edge_weights else 1
        edge_widths = [0.5 + 4 * (w / max_weight) for w in edge_weights]
        edge_alphas = [0.2 + 0.6 * (w / max_weight) for w in edge_weights]
        
        # Draw edges with varying alpha
        for (u, v), width, alpha in zip(G.edges(), edge_widths, edge_alphas):
            nx.draw_networkx_edges(G, pos, edgelist=[(u, v)], 
                                  width=width, alpha=alpha, edge_color='gray', ax=ax)
        
        # Draw nodes
        node_sizes = [400 + 50 * G.degree(node) for node in G.nodes()]
        nx.draw_networkx_nodes(G, pos, node_color=node_colors,
                              node_size=node_sizes, alpha=0.9, ax=ax)
        
        # Draw labels
        nx.draw_networkx_labels(G, pos, font_size=9, font_weight='bold', ax=ax)
        
        # Add frame labels
        ax.text(-1, max(i - len(resp_nodes)/2 for i in range(len(resp_nodes))) + 1.5,
               'RESPONSIBILITY\nFRAME', ha='center', va='bottom', fontsize=12, 
               fontweight='bold', color='#2c3e50')
        ax.text(1, max(i - len(sci_nodes)/2 for i in range(len(sci_nodes))) + 1.5,
               'SCIENCE\nFRAME', ha='center', va='bottom', fontsize=12,
               fontweight='bold', color='#2c3e50')
        
        # Legend
        from matplotlib.patches import Patch
        legend_elements = [
            Patch(facecolor='#2ecc71', label='Credit (Resp+)'),
            Patch(facecolor='#e74c3c', label='Blame (Resp-)'),
            Patch(facecolor='#3498db', label='Pro-Science (Sci+)'),
            Patch(facecolor='#9b59b6', label='Anti-Science (Sci-)'),
        ]
        ax.legend(handles=legend_elements, loc='upper center', 
                 bbox_to_anchor=(0.5, -0.02), ncol=4, fontsize=10)
        
        ax.set_title('Cross-Frame Term Co-occurrence Network\n'
                    'Responsibility ↔ Science Frame Connections',
                    fontsize=14, fontweight='bold')
        ax.axis('off')
        
        plt.tight_layout()
        
        if save:
            path = self.output_dir / 'cross_frame_network.png'
            plt.savefig(path, dpi=300, bbox_inches='tight', facecolor='white')
            print(f"✓ Saved: {path}")
        
        plt.show()
        
        return G
    
    def plot_full_network(self, analysis_results: Dict, 
                         min_weight: int = 2, save: bool = True):
        """
        Create comprehensive network showing all frame term connections.
        """
        import matplotlib.pyplot as plt
        import networkx as nx
        
        builder = FrameNetworkBuilder()
        G = builder.build_full_frame_network(analysis_results, 
                                            min_within=min_weight,
                                            min_cross=min_weight)
        
        if len(G.nodes()) == 0:
            print("Warning: No connections above threshold")
            return
        
        fig, ax = plt.subplots(1, 1, figsize=(20, 16))
        
        # Use spring layout with more spacing
        pos = nx.spring_layout(G, k=3, iterations=100, seed=42)
        
        # Node colors
        node_colors = []
        for node in G.nodes():
            frame = G.nodes[node].get('frame', 'unknown')
            valence = G.nodes[node].get('valence', 'unknown')
            
            if frame == 'responsibility' and valence == 'positive':
                node_colors.append('#2ecc71')
            elif frame == 'responsibility' and valence == 'negative':
                node_colors.append('#e74c3c')
            elif frame == 'science' and valence == 'positive':
                node_colors.append('#3498db')
            elif frame == 'science' and valence == 'negative':
                node_colors.append('#9b59b6')
            else:
                node_colors.append('#95a5a6')
        
        # Edge styling based on type
        within_resp_edges = [(u, v) for u, v, d in G.edges(data=True) 
                            if d.get('edge_type') == 'within_resp']
        within_sci_edges = [(u, v) for u, v, d in G.edges(data=True) 
                           if d.get('edge_type') == 'within_sci']
        cross_edges = [(u, v) for u, v, d in G.edges(data=True) 
                      if d.get('edge_type') == 'cross_frame']
        
        # Draw edges by type
        if within_resp_edges:
            weights = [G[u][v]['weight'] for u, v in within_resp_edges]
            max_w = max(weights)
            widths = [1 + 3 * (w / max_w) for w in weights]
            nx.draw_networkx_edges(G, pos, edgelist=within_resp_edges,
                                  width=widths, alpha=0.5, edge_color='#e74c3c', ax=ax)
        
        if within_sci_edges:
            weights = [G[u][v]['weight'] for u, v in within_sci_edges]
            max_w = max(weights) if weights else 1
            widths = [1 + 3 * (w / max_w) for w in weights]
            nx.draw_networkx_edges(G, pos, edgelist=within_sci_edges,
                                  width=widths, alpha=0.5, edge_color='#3498db', ax=ax)
        
        if cross_edges:
            weights = [G[u][v]['weight'] for u, v in cross_edges]
            max_w = max(weights) if weights else 1
            widths = [0.5 + 2 * (w / max_w) for w in weights]
            nx.draw_networkx_edges(G, pos, edgelist=cross_edges,
                                  width=widths, alpha=0.3, edge_color='#7f8c8d', 
                                  style='dashed', ax=ax)
        
        # Draw nodes
        node_sizes = [300 + 80 * G.degree(node) for node in G.nodes()]
        nx.draw_networkx_nodes(G, pos, node_color=node_colors,
                              node_size=node_sizes, alpha=0.9, ax=ax)
        nx.draw_networkx_labels(G, pos, font_size=8, font_weight='bold', ax=ax)
        
        # Legend
        from matplotlib.patches import Patch
        from matplotlib.lines import Line2D
        
        legend_elements = [
            Patch(facecolor='#2ecc71', label='Responsibility +'),
            Patch(facecolor='#e74c3c', label='Responsibility -'),
            Patch(facecolor='#3498db', label='Science +'),
            Patch(facecolor='#9b59b6', label='Science -'),
            Line2D([0], [0], color='#e74c3c', linewidth=2, label='Within Resp.'),
            Line2D([0], [0], color='#3498db', linewidth=2, label='Within Science'),
            Line2D([0], [0], color='#7f8c8d', linewidth=2, linestyle='--', label='Cross-Frame'),
        ]
        ax.legend(handles=legend_elements, loc='upper left', fontsize=10)
        
        ax.set_title('Complete Frame Term Network\n'
                    'Trump COVID-19 Coverage: Term Co-occurrences',
                    fontsize=14, fontweight='bold')
        ax.axis('off')
        
        # Add network statistics
        stats_text = (f"Nodes: {G.number_of_nodes()}\n"
                     f"Edges: {G.number_of_edges()}\n"
                     f"Density: {nx.density(G):.3f}")
        ax.text(0.02, 0.02, stats_text, transform=ax.transAxes, fontsize=10,
               verticalalignment='bottom', bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
        
        plt.tight_layout()
        
        if save:
            path = self.output_dir / 'full_frame_network.png'
            plt.savefig(path, dpi=300, bbox_inches='tight', facecolor='white')
            print(f"✓ Saved: {path}")
        
        plt.show()
        
        return G


# ============================================================================
# PART 4: MAIN PIPELINE
# ============================================================================

def run_network_analysis(data_path: str = None):
    """
    Run complete network analysis pipeline.
    """
    import networkx as nx
    
    print("="*70)
    print("FRAME NETWORK VISUALIZATION")
    print("Trump COVID-19 Coverage: NYT vs. Guardian")
    print("="*70)
    
    # Load data
    if data_path is None:
        project_root = Path(__file__).parent.parent
        data_path = project_root / 'results' / 'framing_analysis_results.csv'
    
    print(f"\nLoading data from: {data_path}")
    df = pd.read_csv(data_path)
    print(f"Loaded {len(df)} articles")
    
    # Initialize analyzer
    analyzer = FrameTermAnalyzer()
    
    # Analyze corpus
    results = analyzer.analyze_corpus(df)
    
    # Print top terms summary
    print("\n" + "="*70)
    print("TOP FRAME TERMS")
    print("="*70)
    
    top_terms = analyzer.get_top_terms(results['term_freq'], n=10)
    
    for category, terms in top_terms.items():
        if terms:
            print(f"\n{category.upper()}:")
            for term, count in terms:
                print(f"  {term}: {count}")
    
    # Print co-occurrence summary
    print("\n" + "="*70)
    print("TOP CROSS-FRAME CO-OCCURRENCES")
    print("="*70)
    
    top_cross = results['cooccur_cross_frame'].most_common(15)
    for (resp_term, sci_term), count in top_cross:
        print(f"  {resp_term} ↔ {sci_term}: {count}")
    
    # Create visualizations
    print("\n" + "="*70)
    print("GENERATING VISUALIZATIONS")
    print("="*70)
    
    project_root = Path(__file__).parent.parent
    viz = FrameVisualization(output_dir=str(project_root / 'results'))
    
    # Word clouds (combined)
    print("\n1. Creating combined word clouds...")
    try:
        viz.plot_word_clouds(results['term_freq'])
    except ImportError:
        print("   Note: Install 'wordcloud' package for word clouds: pip install wordcloud")
    
    # Word clouds by outlet (NYT vs Guardian)
    print("\n2. Creating word clouds by outlet (NYT vs Guardian)...")
    try:
        outlet_results = analyzer.analyze_corpus_by_outlet(df)
        viz.plot_word_clouds_by_outlet(outlet_results)
    except ImportError:
        print("   Note: Install 'wordcloud' package for word clouds: pip install wordcloud")
    except Exception as e:
        print(f"   Warning: Could not create outlet word clouds: {e}")
    
    # Top terms bar chart
    print("\n3. Creating top terms frequency chart...")
    viz.plot_top_terms_bar(results['term_freq'])
    
    # Cross-frame network
    print("\n4. Creating cross-frame network...")
    viz.plot_cross_frame_network(results['cooccur_cross_frame'], min_weight=2)
    
    # Full network
    print("\n5. Creating full frame network...")
    G = viz.plot_full_network(results, min_weight=2)
    
    # Network statistics
    if G is not None and len(G.nodes()) > 0:
        print("\n" + "="*70)
        print("NETWORK STATISTICS")
        print("="*70)
        print(f"  Total nodes: {G.number_of_nodes()}")
        print(f"  Total edges: {G.number_of_edges()}")
        print(f"  Network density: {nx.density(G):.4f}")
        
        if nx.is_connected(G):
            print(f"  Average path length: {nx.average_shortest_path_length(G):.2f}")
        
        # Top central nodes
        degree_cent = nx.degree_centrality(G)
        top_central = sorted(degree_cent.items(), key=lambda x: x[1], reverse=True)[:10]
        print(f"\n  Most central terms (by degree):")
        for term, cent in top_central:
            print(f"    {term}: {cent:.3f}")
    
    print("\n" + "="*70)
    print("✓ Network analysis complete!")
    print("="*70)
    
    return results, G


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    # Install required packages note
    print("Required packages: networkx, matplotlib, wordcloud")
    print("Install with: pip install networkx wordcloud\n")
    
    results, G = run_network_analysis()

