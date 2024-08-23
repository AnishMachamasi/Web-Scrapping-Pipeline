import io
import csv
import time
from minio import Minio
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup, NavigableString
from webdriver_manager.chrome import ChromeDriverManager

URL = "https://www.amazon.in/s?k=bags&crid=2M096C61O4MLT&qid=1653308124&sprefix=ba%2Caps%2C283&ref=sr_pg_"
MINIO_URL = 'localhost:9000'  # Change this to your MinIO server URL
MINIO_ACCESS_KEY = 'minio'  # Change this to your MinIO access key
MINIO_SECRET_KEY = 'minio123'  # Change this to your MinIO secret key
BUCKET_NAME = 'scraped-data'  # Change this to your MinIO bucket name

# Set up Chrome options
chrome_options = Options()
# chrome_options.add_argument("--headless")  # Ensure GUI is off
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")

# Initialize WebDriver with options and MinIO Client
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
minio_client = Minio(MINIO_URL, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)

# In-memory buffer to store CSV data
csv_buffer = io.StringIO()
csv_writer = csv.DictWriter(csv_buffer, fieldnames=["Product URL", "Product Name", "Product Price", "Rating", "Number of reviews", "Page Number", "Manufacturer", "ASIN"])
csv_writer.writeheader()

def get_urls(start_page, end_page):
    """Generates and returns a list of URLs for the specified page range."""
    return [f"{URL}{page}" for page in range(start_page, end_page + 1)]

def get_data(item, page_number):
    """Extracts product data from a BeautifulSoup item and returns it as a dictionary."""
    try:
        atag = item.h2.a
        req_name = item.h2.text.strip()
        req_url = "https://www.amazon.in" + atag.get("href")
        req_price = item.find('span', 'a-offscreen').text.strip() if item.find('span', 'a-offscreen') else 'N/A'
        req_rating = item.i.text.strip() if item.i else 'N/A'
        req_no_of_ratings = item.find('span', {'class': 'a-size-base s-underline-text'}).text.strip() if item.find('span', {'class': 'a-size-base s-underline-text'}) else 'N/A'
        
        return {
            "Product Name": req_name,
            "Product URL": req_url,
            "Product Price": req_price,
            "Rating": req_rating,
            "Number of reviews": req_no_of_ratings,
            "Page Number": page_number
        }
    except Exception as e:
        print(f"Error extracting data: {e}")
        return None

def extract_additional_details(driver, url):
    """Extracts additional product details from the product page."""
    try:
        driver.get(url)
        time.sleep(2)  # Allow time for the page to load
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        info = soup.find_all('div', {'id': "detailBullets_feature_div"})
        
        if info:
            req_list = info[0].find_all('span', 'a-list-item')
        else:
            info = soup.find_all('table', {'id': "productDetails_techSpec_section_1"})
            info2 = soup.find_all('table', {'id': "productDetails_detailBullets_sections1"})
            req_list = (info[0].find_all('tr') if info else []) + (info2[0].find_all('tr') if info2 else [])

        product_desc_list = ['Manufacturer', 'ASIN']
        req_dict = {}

        for req in req_list:
            contents = req.contents
            key, value = '', ''
            for content in contents:
                if isinstance(content, NavigableString):
                    continue
                text = content.get_text(strip=True)
                if text in product_desc_list:
                    key = text
                else:
                    value = text

            if key:
                req_dict[key] = value

        return req_dict
    except Exception as e:
        print(f"Error extracting additional details: {e}")
        return {}

def save_to_buffer(data):
    """Writes a single record to the in-memory buffer."""
    csv_writer.writerow(data)
    print("Data added to buffer")

def upload_to_minio():
    """Uploads the concatenated CSV data (existing + new) to MinIO."""
    try:
        # Retrieve existing data from MinIO
        existing_csv_data = ""
        try:
            existing_data = minio_client.get_object(BUCKET_NAME, 'Scraped_Data.csv')
            existing_csv_data = existing_data.read().decode('utf-8')
        except Exception as e:
            if 'NoSuchKey' in str(e):
                # If the file does not exist, we'll just work with an empty string
                existing_csv_data = ""
            else:
                print(f"Error retrieving existing data from MinIO: {e}")

        # Read existing data into a StringIO buffer
        existing_buffer = io.StringIO(existing_csv_data)
        existing_reader = csv.DictReader(existing_buffer)

        # Create a new buffer for the combined data
        combined_buffer = io.StringIO()
        fieldnames = existing_reader.fieldnames

        if fieldnames:
            combined_writer = csv.DictWriter(combined_buffer, fieldnames=fieldnames)
            combined_writer.writeheader()
            
            # Write existing data to the combined buffer
            for row in existing_reader:
                combined_writer.writerow(row)
        
            # Write new data to the combined buffer
            csv_buffer.seek(0)  # Go to the start of the new data buffer
            new_csv_data = csv_buffer.getvalue()
            new_buffer = io.StringIO(new_csv_data)
            new_reader = csv.DictReader(new_buffer, fieldnames=fieldnames)
            
            # Skip the header in new data
            next(new_reader)
            
            for row in new_reader:
                combined_writer.writerow(row)
            
            combined_buffer.seek(0)  # Go to the start of the combined buffer
            combined_data = combined_buffer.getvalue().encode('utf-8')
            
            # Upload the combined data to MinIO
            minio_client.put_object(BUCKET_NAME, 'Scraped_Data.csv', io.BytesIO(combined_data), len(combined_data))
            print("Data successfully uploaded to MinIO")
        
        else:
            print("No fieldnames found. Existing data may be empty or malformed.")
            
    except Exception as e:
        print(f"Error uploading to MinIO: {e}")

def get_existing_pages():
    """Returns a set of page numbers already in the CSV file stored in MinIO."""
    existing_pages = set()
    try:
        objects = minio_client.list_objects(BUCKET_NAME, recursive=True)
        for obj in objects:
            if obj.object_name == 'Scraped_Data.csv':
                data = minio_client.get_object(BUCKET_NAME, obj.object_name).read().decode('utf-8')
                reader = csv.DictReader(io.StringIO(data))
                for row in reader:
                    if 'Page Number' in row and row['Page Number'].isdigit():
                        existing_pages.add(int(row['Page Number']))
    except Exception as e:
        print(f"Error retrieving existing pages from MinIO: {e}")
    return existing_pages

def main():
    """Main function to scrape data from a specified page and save incrementally to CSV and MinIO."""
    try:
        # Only scrape a single page
        page_number_to_scrape = 1  # Change this value to the page you want to scrape
        existing_pages = get_existing_pages()

        # Generate the list of scraped pages
        scraped_pages = list(range(1, max(existing_pages) + 1))

        if page_number_to_scrape in existing_pages:
            scraped_pages_str = ", ".join(map(str, scraped_pages))
            message = f"Page {scraped_pages_str} has already been scraped. So scraping next page."
            page_number_to_scrape = max(existing_pages) + 1
            print(message)
        else:
            print("No pages have been scraped yet.")
        
        page_url = f"{URL}{page_number_to_scrape}"
        print(f"Scraping page: {page_url}")
        driver.get(page_url)
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        results = soup.find_all('div', {'data-component-type': "s-search-result"})
        
        for item in results:
            product_data = get_data(item, page_number_to_scrape)
            if product_data:
                additional_details = extract_additional_details(driver, product_data["Product URL"])
                product_data.update(additional_details)
                save_to_buffer(product_data)  # Save each item immediately to buffer
        
        # Upload the CSV buffer to MinIO
        upload_to_minio()
        
    except Exception as e:
        print(f"Error in main function: {e}")
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
