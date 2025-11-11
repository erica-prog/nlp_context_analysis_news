import os
import pandas as pd
import requests
import time
import datetime
import glob
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from config_ny import API_KEY

API_KEY = API_KEY

def search_trump_covid_comprehensive(year, month, page=0):
    """
    Use multiple queries to get comprehensive Trump + COVID coverage
    Using NYTimes capitalization style: "Covid" not "COVID"
    """
    base_url = 'https://api.nytimes.com/svc/search/v2/articlesearch.json'
    
    start_date = datetime.date(year, month, 1)
    if month == 12:
        end_date = datetime.date(year + 1, 1, 1) - datetime.timedelta(days=1)
    else:
        end_date = datetime.date(year, month + 1, 1) - datetime.timedelta(days=1)
    
    # Multiple queries with BOTH Covid and COVID variations
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
    
    all_docs = []
    seen_urls = set()
    
    for i, query in enumerate(queries):
        params = {
            'q': query,
            'begin_date': start_date.strftime('%Y%m%d'),
            'end_date': end_date.strftime('%Y%m%d'),
            'api-key': API_KEY,
            'page': page,
            'sort': 'newest'
        }
        
        try:
            if i > 0:
                time.sleep(2)
            
            response = requests.get(base_url, params=params, verify=False, timeout=30)
            
            if response.status_code == 429:
                print(f" [Q{i+1} rate limit]", end="")
                time.sleep(60)
                continue
            
            if response.status_code != 200:
                continue
            
            json_response = response.json()
            
            if 'response' in json_response and 'docs' in json_response['response']:
                docs = json_response['response']['docs']
                for doc in docs:
                    url = doc.get('web_url')
                    if url and url not in seen_urls:
                        all_docs.append(doc)
                        seen_urls.add(url)
        
        except Exception as e:
            continue
    
    if all_docs:
        return {'response': {'docs': all_docs}}
    return None


def search_trump_covid_single(year, month, page=0):
    """Single comprehensive query for pagination with both capitalizations"""
    base_url = 'https://api.nytimes.com/svc/search/v2/articlesearch.json'
    
    start_date = datetime.date(year, month, 1)
    if month == 12:
        end_date = datetime.date(year + 1, 1, 1) - datetime.timedelta(days=1)
    else:
        end_date = datetime.date(year, month + 1, 1) - datetime.timedelta(days=1)
    
    # Include both Covid and COVID to catch all variations
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
        'begin_date': start_date.strftime('%Y%m%d'),
        'end_date': end_date.strftime('%Y%m%d'),
        'api-key': API_KEY,
        'page': page,
        'sort': 'newest'
    }
    
    try:
        response = requests.get(base_url, params=params, verify=False, timeout=30)
        
        if response.status_code == 429:
            print(" Rate limit!", end="")
            time.sleep(60)
            return None
        
        if response.status_code != 200:
            return None
        
        return response.json()
        
    except Exception as e:
        return None


def parse_articles(response):
    """Parse API response and extract article data"""
    if not response or 'response' not in response or 'docs' not in response['response']:
        return pd.DataFrame()
    
    articles = response['response']['docs']
    if not articles:
        return pd.DataFrame()
    
    data = []
    for article in articles:
        try:
            row = {
                'headline': article.get('headline', {}).get('main', ''),
                'pub_date': article.get('pub_date', ''),
                'snippet': article.get('snippet', ''),
                'web_url': article.get('web_url', ''),
                'word_count': article.get('word_count', 0),
                'abstract': article.get('abstract', ''),
                'news_desk': article.get('news_desk', ''),
                'section_name': article.get('section_name', ''),
                'subsection_name': article.get('subsection_name', ''),
                'type_of_material': article.get('type_of_material', ''),
                'byline': article.get('byline', {}).get('original', '') if isinstance(article.get('byline'), dict) else '',
                'keywords': ', '.join([kw['value'] for kw in article.get('keywords', []) if kw.get('name') == 'subject'])
            }
            data.append(row)
        except:
            continue
    
    return pd.DataFrame(data)


def get_all_articles_for_month(year, month):
    """Fetch all articles for a given month using comprehensive approach"""
    all_articles = []
    
    # Step 1: Multi-query comprehensive search (page 0 only)
    print(f"  Multi-query search (5 queries)...", end=" ")
    response = search_trump_covid_comprehensive(year, month, page=0)
    
    if response:
        df = parse_articles(response)
        if len(df) > 0:
            all_articles.append(df)
            print(f"{len(df)} unique articles ✓")
        else:
            print("No results")
    else:
        print("✗")
    
    # Step 2: Paginate with single query for additional pages
    for page in range(1, 15):  # Up to 15 additional pages
        time.sleep(7)  # Rate limit
        
        print(f"  Page {page}...", end=" ")
        response = search_trump_covid_single(year, month, page=page)
        
        if not response:
            print("✗")
            break
        
        df = parse_articles(response)
        
        if len(df) == 0:
            print("Done")
            break
        
        all_articles.append(df)
        print(f"{len(df)} articles ✓")
        
        if len(df) < 10:
            break
    
    # Combine and deduplicate
    if all_articles:
        combined = pd.concat(all_articles, ignore_index=True)
        combined = combined.drop_duplicates(subset=['web_url'], keep='first')
        return combined
    
    return pd.DataFrame()


def scrape_by_month(start_date, end_date):
    """Scrape articles month by month"""
    if not os.path.exists('nytimes_trump_covid'):
        os.mkdir('nytimes_trump_covid')
    
    current = start_date
    total = 0
    monthly_counts = []
    
    while current <= end_date:
        year, month = current.year, current.month
        
        print(f"\n{'='*60}")
        print(f"📅 {year}-{month:02d}")
        print(f"{'='*60}")
        
        csv_path = f'nytimes_trump_covid/{year}-{month:02d}.csv'
        
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


# Main execution - Full 2020-2025 coverage - it has exhausted API limits so start from 2023-08-01
start = datetime.date(2023, 8, 1)
end = datetime.date.today()

print(f"🔍 Scraping NYTimes: Trump & COVID-19")
print(f"📅 Period: {start} → {end}")
print(f"🎯 Strategy: Multi-query comprehensive search + pagination")
print(f"📊 Expected: High article counts across all months\n")

scrape_by_month(start, end)

# Combine all files
print(f"\n{'='*60}")
print("📊 COMBINING ALL FILES")
print(f"{'='*60}")

files = sorted(glob.glob('nytimes_trump_covid/*.csv'))

if files:
    combined = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)
    
    # Final deduplication
    print(f"Before deduplication: {len(combined)} articles")
    combined = combined.drop_duplicates(subset=['web_url'], keep='first')
    print(f"After deduplication: {len(combined)} unique articles")
    
    combined['pub_date'] = pd.to_datetime(combined['pub_date'])
    combined = combined.sort_values('pub_date', ascending=False)
    
    combined.to_csv("nytimes_trump_covid/combined.csv", index=False)
    
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
    print(combined['section_name'].value_counts().head(7))
    
    print(f"\n📝 Sample recent headlines:")
    for i, (_, row) in enumerate(combined.head(5).iterrows(), 1):
        print(f"\n  {i}. {row['headline']}")
        print(f"     {row['pub_date'].date()} | {row['section_name']}")
    
    print(f"\n💾 Saved to: nytimes_trump_covid/combined.csv")
    print(f"📁 Monthly files: nytimes_trump_covid/YYYY-MM.csv")
else:
    print("❌ No files found")