import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import openai
from langchain.chains import create_extraction_chain
from langchain.chat_models import ChatOpenAI
import csv
import os
from dotenv import load_dotenv

load_dotenv()

# Define the URL to scrape
url = ""

# Define the schema for LLM extraction
schema = {
    "properties": {
        "business_name": {"type": "string"},
        "phone_number": {"type": "string"},
        "address": {"type": "string", "format": "uri"},  
    },
    "required": ["business_name", "phone_number", "address"],
}

# Set up your OpenAI API key
openai.api_key = os.getenv("OPENAI_API_KEY")

async def scrape_with_playwright(url, schema):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(url)

        # Wait for the 'Load More' button to be available and clickable
        load_more_button_selector = 'm-811560b9 mantine-Button-label'  # Updated selector for the 'Load More' button
        try:
            # Wait for the selector to be available
            await page.wait_for_selector(load_more_button_selector, state='attached', timeout=60000)
            # Click the button
            await page.click(load_more_button_selector)
            # Wait for some time to allow content to load
            await asyncio.sleep(5)
        except Exception as e:
            print(f"Error occurred: {e}")

        html_content = await page.content()
        await browser.close()

        soup = BeautifulSoup(html_content, 'html.parser')
        tags_to_extract = ['h1', 'h2', 'h3', 'p', 'li', 'div', 'span', 'a']
        text_content = ' '.join([element.get_text() for tag in tags_to_extract for element in soup.find_all(tag)])

        llm = ChatOpenAI(temperature=0, model="gpt-3.5-turbo-16k")
        extracted_content = create_extraction_chain(schema=schema, llm=llm).run(text_content)

        return extracted_content

extracted_content = asyncio.run(scrape_with_playwright(url, schema))

csv_file = 'gpt_assistants_data.csv'
with open(csv_file, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.DictWriter(file, fieldnames=['business_name', 'phone_number', 'address'])
    writer.writeheader()
    for item in extracted_content:
        writer.writerow(item)

print(f"Data written to {csv_file}")
