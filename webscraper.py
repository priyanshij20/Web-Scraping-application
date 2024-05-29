import requests
from bs4 import BeautifulSoup
import logging
import pymongo
import json

# Set up logging
logging.basicConfig(filename='scraping_errors.log', level=logging.DEBUG, format='%(asctime)s:%(levelname)s:%(message)s')

# MongoDB connection
def get_mongo_client():
    try:
        client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = client["scraped_data"]
        logging.debug("Connected to MongoDB")
        return db
    except Exception as e:
        logging.error(f"Error connecting to MongoDB: {e}")
        raise

# Fetch page content with headers
def fetch_page(url, is_json=False):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        logging.debug(f"Successfully fetched URL: {url}")
        if is_json:
            logging.debug(f"Raw response content: {response.text[:1000]}")  # Log the first 1000 characters of the response
            try:
                return response.json()
            except json.JSONDecodeError as e:
                logging.error(f"JSON decoding error: {e}")
                logging.debug(f"Raw response that caused JSON error: {response.text}")
                return None
        logging.debug(f"Raw HTML content: {response.text[:1000]}")  # Log the first 1000 characters of the HTML response
        return response.text
    except requests.RequestException as e:
        logging.error(f"Error fetching {url}: {e}")
        return None

# Scrape AJAX page
def scrape_ajax_page():
    url = "https://www.scrapethissite.com/pages/ajax-javascript/?ajax=true&year=2015"  # Update this URL
    data = fetch_page(url, is_json=True)
    if data:
        logging.debug(f"AJAX Data: {data}")
        return data
    logging.error("Failed to fetch AJAX data")
    return None

# Scrape forms page
def scrape_forms_page():
    url = "https://www.scrapethissite.com/pages/forms/"
    html_content = fetch_page(url)
    if html_content:
        soup = BeautifulSoup(html_content, 'html.parser')
        data = []
        # Log the targeted HTML section for verification
        teams = soup.select("div.team")
        logging.debug(f"Team HTML sections: {[str(team) for team in teams]}")

        for team in teams:
            try:
                team_name = team.find("h3").text.strip()
                year = team.find("span.year").text.strip()
                wins = team.find("span.wins").text.strip()
                losses = team.find("span.losses").text.strip()
                data.append({"team_name": team_name, "year": year, "wins": wins, "losses": losses})
            except AttributeError as e:
                logging.error(f"Error extracting team data: {e}")
                logging.debug(f"Problematic HTML section: {str(team)}")
        logging.debug(f"Forms Data: {data}")
        return data
    logging.error("Failed to fetch forms data")
    return None

# Scrape advanced page
def scrape_advanced_page():
    url = "https://www.scrapethissite.com/pages/advanced/"
    html_content = fetch_page(url)
    if html_content:
        soup = BeautifulSoup(html_content, 'html.parser')
        data = []
        # Extract advanced data
        for country in soup.select("div.country"):
            country_name = country.find("h3").text.strip()
            data.append({"country_name": country_name})
        logging.debug(f"Advanced Data: {data}")
        return data
    logging.error("Failed to fetch advanced data")
    return None

# Save data to MongoDB
def save_to_mongo(collection_name, data):
    try:
        collection = db[collection_name]
        if isinstance(data, list):
            if data:  # Ensure there's data to insert
                logging.debug(f"Inserting {len(data)} documents into {collection_name} collection")
                result = collection.insert_many(data)
                logging.debug(f"Inserted document IDs: {result.inserted_ids}")
            else:
                logging.debug(f"No data to insert into {collection_name} collection")
        else:
            logging.debug(f"Inserting 1 document into {collection_name} collection")
            result = collection.insert_one(data)
            logging.debug(f"Inserted document ID: {result.inserted_id}")
    except Exception as e:
        logging.error(f"Error saving to MongoDB: {e}")

# Main function
def main():
    ajax_data = scrape_ajax_page()
    if ajax_data:
        save_to_mongo("ajax_data", ajax_data)
    
    forms_data = scrape_forms_page()
    if forms_data:
        logging.debug(f"Forms Data to Save: {forms_data}")
        save_to_mongo("forms_data", forms_data)
    
    advanced_data = scrape_advanced_page()
    if advanced_data:
        logging.debug(f"Advanced Data to Save: {advanced_data}")
        save_to_mongo("advanced_data", advanced_data)
    logging.debug("Done")

if __name__ == "__main__":
    db = get_mongo_client()
    main()