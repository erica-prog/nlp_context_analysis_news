"""
NY Times News Scraper using NewsAPI
Fetches articles from January 1, 2020 to December 31, 2022
"""

import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import json
from config_news import NEWS_API_KEY

# NewsAPI base URL
BASE_URL = "https://newsapi.org/v2/everything"

def fetch_nytimes_articles(start_date, end_date, page=1, page_size=100):
    """
    Fetch NY Times articles for a given date range.
    
    Args:
        start_date: Start date in 'YYYY-MM-DD' format
        end_date: End date in 'YYYY-MM-DD' format
        page: Page number for pagination
        page_size: Number of results per page (max 100)
    
    Returns:
        JSON response from NewsAPI
    """
    params = {
        'apiKey': NEWS_API_KEY,
        'domains': 'nytimes.com',
        'from': start_date,
        'to': end_date,
        'language': 'en',
        'sortBy': 'publishedAt',
        'page': page,
        'pageSize': page_size
    }
    
    response = requests.get(BASE_URL, params=params)
    return response.json()


def scrape_nytimes_news(start_date, end_date, output_file='nytimes_articles.csv'):
    """
    Scrape NY Times news articles for the entire date range.
    
    Note: NewsAPI free tier only allows searching articles from the last month.
    For historical data (2020-2022), you'll need a paid plan.
    
    Args:
        start_date: Start date string 'YYYY-MM-DD'
        end_date: End date string 'YYYY-MM-DD'
        output_file: Output CSV filename
    """
    all_articles = []
    
    # Convert to datetime objects
    current_start = datetime.strptime(start_date, '%Y-%m-%d')
    final_end = datetime.strptime(end_date, '%Y-%m-%d')
    
    # NewsAPI allows max 1 month range per request for better results
    # We'll iterate month by month
    while current_start < final_end:
        # Calculate end of current month or final end date
        current_end = min(
            current_start + timedelta(days=30),
            final_end
        )
        
        start_str = current_start.strftime('%Y-%m-%d')
        end_str = current_end.strftime('%Y-%m-%d')
        
        print(f"\nFetching articles from {start_str} to {end_str}...")
        
        page = 1
        total_results = 0
        
        while True:
            print(f"  Page {page}...")
            
            try:
                response = fetch_nytimes_articles(start_str, end_str, page=page)
                
                if response.get('status') == 'error':
                    print(f"  Error: {response.get('message', 'Unknown error')}")
                    break
                
                articles = response.get('articles', [])
                total_results = response.get('totalResults', 0)
                
                if not articles:
                    print(f"  No more articles found.")
                    break
                
                # Process each article
                for article in articles:
                    article_data = {
                        'source': article.get('source', {}).get('name', ''),
                        'author': article.get('author', ''),
                        'title': article.get('title', ''),
                        'description': article.get('description', ''),
                        'url': article.get('url', ''),
                        'published_at': article.get('publishedAt', ''),
                        'content': article.get('content', '')
                    }
                    all_articles.append(article_data)
                
                print(f"  Retrieved {len(articles)} articles. Total so far: {len(all_articles)}")
                
                # Check if we've retrieved all available articles
                # NewsAPI free tier limits to 100 results total
                if page * 100 >= total_results or page * 100 >= 100:
                    break
                
                page += 1
                
                # Rate limiting - be nice to the API
                time.sleep(1)
                
            except Exception as e:
                print(f"  Exception occurred: {str(e)}")
                break
        
        # Move to next month
        current_start = current_end + timedelta(days=1)
        
        # Rate limiting between date ranges
        time.sleep(1)
    
    # Convert to DataFrame and save
    if all_articles:
        df = pd.DataFrame(all_articles)
        
        # Convert published_at to datetime
        df['published_at'] = pd.to_datetime(df['published_at'])
        
        # Sort by date
        df = df.sort_values('published_at')
        
        # Save to CSV
        df.to_csv(output_file, index=False)
        print(f"\n{'='*50}")
        print(f"Successfully saved {len(df)} articles to '{output_file}'")
        print(f"{'='*50}")
        
        # Display summary statistics
        print(f"\nSummary:")
        print(f"  - Total articles: {len(df)}")
        print(f"  - Date range: {df['published_at'].min()} to {df['published_at'].max()}")
        print(f"  - Unique authors: {df['author'].nunique()}")
        
        return df
    else:
        print("\nNo articles were retrieved.")
        return None


def main():
    """Main function to run the scraper."""
    print("="*60)
    print("NY Times News Scraper using NewsAPI")
    print("="*60)
    print(f"\nAPI Key: {NEWS_API_KEY[:10]}...{NEWS_API_KEY[-4:]}")
    
    # Define date range
    START_DATE = "2020-01-01"
    END_DATE = "2022-12-31"
    
    print(f"\nTarget date range: {START_DATE} to {END_DATE}")
    print("\n⚠️  IMPORTANT NOTE:")
    print("   NewsAPI FREE tier only allows searching articles from the LAST MONTH.")
    print("   For historical data (2020-2022), you need a PAID subscription.")
    print("   See: https://newsapi.org/pricing")
    print()
    
    # Run the scraper
    df = scrape_nytimes_news(START_DATE, END_DATE)
    
    if df is not None:
        print("\nFirst 5 articles:")
        print(df[['title', 'published_at']].head())


if __name__ == "__main__":
    main()

