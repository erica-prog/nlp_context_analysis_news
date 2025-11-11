import requests
import json
from config_ny import API_KEY

API_KEY = API_KEY

# Test 1: Simple search
print("="*60)
print("TEST 1: Testing NYTimes Article Search API")
print("="*60)

url = 'https://api.nytimes.com/svc/search/v2/articlesearch.json'
params = {
    'q': 'artificial intelligence jobs',
    'begin_date': '20240101',
    'end_date': '20240131',
    'api-key': API_KEY
}

response = requests.get(url, params=params)

print(f"Status Code: {response.status_code}")
print(f"\nResponse Keys: {response.json().keys()}")

if response.status_code == 200:
    data = response.json()
    print(f"\nStatus: {data.get('status')}")
    
    if 'response' in data:
        print(f"Response Keys: {data['response'].keys()}")
        
        if 'meta' in data['response']:
            print(f"Total Hits: {data['response']['meta']['hits']}")
        
        if 'docs' in data['response']:
            print(f"Articles in this response: {len(data['response']['docs'])}")
            
            if len(data['response']['docs']) > 0:
                article = data['response']['docs'][0]
                print(f"\nSample Article:")
                print(f"  Headline: {article.get('headline', {}).get('main', 'N/A')}")
                print(f"  Date: {article.get('pub_date', 'N/A')}")
                print(f"  URL: {article.get('web_url', 'N/A')}")
                print("\n✅ API is working!")
            else:
                print("\n⚠️ No articles found for this query")
    else:
        print("\n❌ No 'response' key in data")
        print(f"Full response: {json.dumps(data, indent=2)[:500]}")
else:
    print(f"\n❌ API Error")
    print(f"Response: {response.text[:500]}")

# Test 2: Check API key validity
print("\n" + "="*60)
print("TEST 2: Checking API Key Status")
print("="*60)

if response.status_code == 401:
    print("❌ API Key is invalid or expired")
    print("Go to: https://developer.nytimes.com/my-apps")
    print("Check if your key is still active")
elif response.status_code == 429:
    print("⚠️ Rate limit exceeded")
    print("You've made too many requests. Wait a bit and try again.")
elif response.status_code == 200:
    print("✅ API Key is valid and working!")
else:
    print(f"⚠️ Unexpected status code: {response.status_code}")