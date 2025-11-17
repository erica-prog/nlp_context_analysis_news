import os
import pandas as pd
import requests
import time
import datetime
import glob
from config_guardian import GUARDIAN_API_KEY

API_KEY = GUARDIAN_API_KEY

def search_trump_covid_comprehensive(from_date, to_date, page=1):
    """
    Use multiple queries to get comprehensive Trump + COVID coverage from Guardian
    Matches NYTimes query structure exactly
    """
    base_url = 'https://content.guardianapis.com/search'
    
    # Exact same queries as NYTimes scraper
    queries = [
        # Query 1: Broad comprehensive query (both variations)
        '(Trump OR "Donald Trump" OR "President Trump" OR "White House" OR '
        '"Trump administration" OR Pence) '
        'AND '
        '(Covid OR COVID OR "Covid-19" OR "COVID-19" OR coronavirus OR pandemic OR vaccine OR '
        'Fauci OR CDC OR masks OR lockdown OR testing OR cases OR deaths OR '
        'hospitalization OR "Walter Reed" OR briefing OR relief OR stimulus OR '
        '"task force" OR hydroxychloroquine OR treatment OR rally OR election)',
        
        # Query 2: Trump's personal COVID experience
        '(Trump OR "Donald Trump") AND (Covid OR COVID OR coronavirus) AND '
        '(hospital OR treatment OR infected OR positive OR masks OR statements OR sick OR diagnosis)',
        
        # Query 3: Administration response & policy
        '("Trump administration" OR "White House" OR Pence OR "federal government") AND '
        '(pandemic OR Covid OR COVID) AND (response OR policy OR briefing OR "task force" OR CDC OR Fauci)',
        
        # Query 4: Economic & political impact
        '(Trump OR "President Trump") AND (coronavirus OR pandemic OR "Covid-19" OR "COVID-19") AND '
        '(vaccine OR relief OR stimulus OR lockdown OR economy OR election OR "Operation Warp Speed")',
        
        # Query 5: Public health measures
        '(Trump OR "Trump administration" OR "White House") AND '
        '(Covid OR COVID OR coronavirus OR pandemic) AND '
        '(quarantine OR "social distancing" OR "public health" OR restrictions OR reopening)'
    ]
    
    all_articles = []
    seen_ids = set()
    
    for i, query in enumerate(queries):
        params = {
            'q': query,
            'from-date': from_date,
            'to-date': to_date,
            'page': page,
            'page-size': 50,
            'order-by': 'newest',
            'show-fields': 'headline,trailText,body,byline,wordcount,thumbnail,standfirst',
            'show-tags': 'keyword,contributor',
            'api-key': API_KEY
        }
        
        try:
            if i > 0:
                time.sleep(1)  # Brief pause between queries
            
            response = requests.get(base_url, params=params, timeout=30)
            
            if response.status_code == 429:
                print(f" [Q{i+1} rate limit]", end="")
                time.sleep(10)
                continue
            
            if response.status_code != 200:
                print(f" [Q{i+1} error {response.status_code}]", end="")
                continue
            
            json_response = response.json()
            
            if 'response' in json_response and 'results' in json_response['response']:
                results = json_response['response']['results']
                for article in results:
                    article_id = article.get('id')
                    if article_id and article_id not in seen_ids:
                        all_articles.append(article)
                        seen_ids.add(article_id)
        
        except Exception as e:
            print(f" [Q{i+1} error: {e}]", end="")
            continue
    
    if all_articles:
        return {'response': {'results': all_articles}}
    return None


def search_trump_covid_single(from_date, to_date, page=1):
    """Single comprehensive query for pagination"""
    base_url = 'https://content.guardianapis.com/search'
    
    # Single broad query for pagination
    query = (
        '(Trump OR "Donald Trump" OR "President Trump" OR "White House" OR '
        '"Trump administration" OR Pence) '
        'AND '
        '(Covid OR COVID OR "Covid-19" OR "COVID-19" OR coronavirus OR pandemic OR vaccine OR '
        'Fauci OR CDC OR masks OR lockdown OR testing OR cases OR deaths OR '
        'hospitalization OR briefing OR relief OR stimulus OR treatment)'
    )
    
    params = {
        'q': query,
        'from-date': from_date,
        'to-date': to_date,
        'page': page,
        'page-size': 50,
        'order-by': 'newest',
        'show-fields': 'headline,trailText,body,byline,wordcount,thumbnail,standfirst',
        'show-tags': 'keyword,contributor',
        'api-key': API_KEY
    }
    
    try:
        response = requests.get(base_url, params=params, timeout=30)
        
        if response.status_code == 429:
            print(" Rate limit!", end="")
            time.sleep(10)
            return None
        
        if response.status_code != 200:
            print(f" Error {response.status_code}", end="")
            return None
        
        return response.json()
        
    except Exception as e:
        print(f" Error: {e}", end="")
        return None


def parse_guardian_articles(response):
    """Parse Guardian API response and extract article data"""
    if not response or 'response' not in response or 'results' not in response['response']:
        return pd.DataFrame()
    
    articles = response['response']['results']
    if not articles:
        return pd.DataFrame()
    
    data = []
    for article in articles:
        try:
            fields = article.get('fields', {})
            
            # Extract snippet/standfirst (Guardian equivalent of NYT snippet)
            snippet = fields.get('standfirst', '') or fields.get('trailText', '')
            
            # Extract keywords
            tags = article.get('tags', [])
            keywords = ', '.join([tag.get('webTitle', '') for tag in tags if tag.get('type') == 'keyword'])
            
            row = {
                'headline': fields.get('headline', article.get('webTitle', '')),
                'pub_date': article.get('webPublicationDate', ''),
                'snippet': snippet,
                'web_url': article.get('webUrl', ''),
                'word_count': fields.get('wordcount', 0),
                'abstract': snippet,  # Guardian uses standfirst/trailText as abstract
                'section_name': article.get('sectionName', ''),
                'pillar_name': article.get('pillarName', ''),
                'type': article.get('type', ''),
                'byline': fields.get('byline', ''),
                'keywords': keywords,
                'article_id': article.get('id', '')
            }
            data.append(row)
        except Exception as e:
            continue
    
    return pd.DataFrame(data)


def get_all_articles_for_month(year, month):
    """Fetch all articles for a given month using comprehensive multi-query approach"""
    all_articles = []
    
    # Date range for the month
    start_date = datetime.date(year, month, 1)
    if month == 12:
        end_date = datetime.date(year + 1, 1, 1) - datetime.timedelta(days=1)
    else:
        end_date = datetime.date(year, month + 1, 1) - datetime.timedelta(days=1)
    
    from_date_str = start_date.strftime('%Y-%m-%d')
    to_date_str = end_date.strftime('%Y-%m-%d')
    
    # Step 1: Multi-query comprehensive search (page 1 only)
    print(f"  Multi-query search (5 queries)...", end=" ")
    response = search_trump_covid_comprehensive(from_date_str, to_date_str, page=1)
    
    if response:
        df = parse_guardian_articles(response)
        if len(df) > 0:
            all_articles.append(df)
            print(f"{len(df)} unique articles ✓")
        else:
            print("No results")
    else:
        print("✗")
    
    # Step 2: Paginate with single query for additional pages
    page = 2
    max_pages = 20  # Guardian pagination limit
    
    while page <= max_pages:
        time.sleep(1)  # Rate limiting
        
        print(f"  Page {page}...", end=" ")
        response = search_trump_covid_single(from_date_str, to_date_str, page=page)
        
        if not response:
            print("✗")
            break
        
        # Check pagination info
        response_obj = response.get('response', {})
        total_pages = response_obj.get('pages', 0)
        current_page = response_obj.get('currentPage', 0)
        
        df = parse_guardian_articles(response)
        
        if len(df) == 0:
            print("No more results")
            break
        
        all_articles.append(df)
        print(f"{len(df)} articles ✓")
        
        # Check if this is the last page
        if current_page >= total_pages:
            print(f"  (Last page: {total_pages})")
            break
        
        page += 1
    
    # Combine and deduplicate
    if all_articles:
        combined = pd.concat(all_articles, ignore_index=True)
        combined = combined.drop_duplicates(subset=['article_id'], keep='first')
        return combined
    
    return pd.DataFrame()


def scrape_by_month(start_date, end_date):
    """Scrape Guardian articles month by month"""
    output_dir = 'guardian_trump_covid'
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    
    current = start_date
    total = 0
    monthly_counts = []
    
    while current <= end_date:
        year, month = current.year, current.month
        
        print(f"\n{'='*60}")
        print(f"📅 {year}-{month:02d}")
        print(f"{'='*60}")
        
        csv_path = f'{output_dir}/{year}-{month:02d}.csv'
        
        if os.path.exists(csv_path):
            df = pd.read_csv(csv_path)
            count = len(df)
            total += count
            monthly_counts.append({'year_month': f'{year}-{month:02d}', 'count': count})
            print(f"  ✅ Already have {count} articles")
        else:
            df = get_all_articles_for_month(year, month)
            
            if len(df) > 0:
                df.to_csv(csv_path, index=False)
                count = len(df)
                total += count
                monthly_counts.append({'year_month': f'{year}-{month:02d}', 'count': count})
                print(f"  ✅ Saved {count} unique articles")
            else:
                monthly_counts.append({'year_month': f'{year}-{month:02d}', 'count': 0})
                print(f"  ⚠️ No articles found")
        
        # Next month
        if month == 12:
            current = datetime.date(year + 1, 1, 1)
        else:
            current = datetime.date(year, month + 1, 1)
    
    print(f"\n{'='*60}")
    print(f"🎉 Total: {total} articles")
    print(f"{'='*60}")
    
    # Monthly breakdown
    print("\n📊 Articles per month:")
    monthly_df = pd.DataFrame(monthly_counts)
    for _, row in monthly_df.iterrows():
        bar = '█' * (row['count'] // 20)
        print(f"  {row['year_month']}: {row['count']:4d} {bar}")


# Main execution - 2020-2023 coverage
start = datetime.date(2020, 1, 1)
end = datetime.date(2023, 12, 31)

print(f"🔍 Scraping The Guardian: Trump & COVID-19")
print(f"📅 Period: {start} → {end}")
print(f"🎯 Strategy: Multi-query comprehensive search (5 queries) + pagination")
print(f"📊 Guardian API: 50 results per page, rate limit friendly\n")

scrape_by_month(start, end)

# Combine all files
print(f"\n{'='*60}")
print("📊 COMBINING ALL FILES")
print(f"{'='*60}")

output_dir = 'guardian_trump_covid'
files = sorted(glob.glob(f'{output_dir}/*.csv'))

if files:
    combined = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)
    
    # Final deduplication
    print(f"Before deduplication: {len(combined)} articles")
    combined = combined.drop_duplicates(subset=['article_id'], keep='first')
    print(f"After deduplication: {len(combined)} unique articles")
    
    combined['pub_date'] = pd.to_datetime(combined['pub_date'])
    combined = combined.sort_values('pub_date', ascending=False)
    
    combined.to_csv(f"{output_dir}/combined.csv", index=False)
    
    print(f"\n✅ {len(combined)} unique articles")
    print(f"📅 {combined['pub_date'].min().date()} → {combined['pub_date'].max().date()}")
    
    # Year breakdown
    combined['year'] = combined['pub_date'].dt.year
    print(f"\n📈 Articles by year:")
    year_counts = combined['year'].value_counts().sort_index()
    for year, count in year_counts.items():
        bar = '█' * (count // 50)
        print(f"  {year}: {count:4d} {bar}")
    
    print(f"\n📰 Top sections:")
    print(combined['section_name'].value_counts().head(10))
    
    print(f"\n🏛️ Top pillars:")
    print(combined['pillar_name'].value_counts().head(5))
    
    print(f"\n📝 Sample recent headlines:")
    for i, (_, row) in enumerate(combined.head(5).iterrows(), 1):
        print(f"\n  {i}. {row['headline']}")
        print(f"     {row['pub_date'].date()} | {row['section_name']}")
    
    print(f"\n💾 Saved to: {output_dir}/combined.csv")
    print(f"📁 Monthly files: {output_dir}/YYYY-MM.csv")
    
    # Export comparison summary
    print(f"\n📊 DATASET SUMMARY")
    print(f"{'='*60}")
    print(f"Total unique articles: {len(combined)}")
    print(f"Date range: {combined['pub_date'].min().date()} to {combined['pub_date'].max().date()}")
    print(f"Average word count: {combined['word_count'].mean():.0f}")
    print(f"Articles with bylines: {combined['byline'].notna().sum()}")
    print(f"Unique sections: {combined['section_name'].nunique()}")
else:
    print("❌ No files found")
