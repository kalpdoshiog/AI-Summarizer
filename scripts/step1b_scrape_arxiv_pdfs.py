import requests
from bs4 import BeautifulSoup
import os
import time
import datetime
import csv

# ---- Folder Structure ----
category = "cs.CV"  # or cs.AI etc.
base_folder = "data/scraped_arxiv_pdfs"
pdf_folder = os.path.join(base_folder, "pdfs")
log_folder = os.path.join(base_folder, "logs")
os.makedirs(pdf_folder, exist_ok=True)
os.makedirs(log_folder, exist_ok=True)

metadata_csv = os.path.join(base_folder, "arxiv_metadata.csv")
log_file = os.path.join(log_folder, "download_log.txt")

# ---- Scraping Setup ----
base_url = f"https://arxiv.org/list/{category}/recent"
base_pdf_url = "https://arxiv.org/pdf/"
per_page = 2000

response = requests.get(f"{base_url}?show={per_page}")
soup = BeautifulSoup(response.text, "html.parser")

raw_ids = [a.text for a in soup.find_all('a', title='Abstract')]
paper_ids = [raw_id.strip().replace("arXiv:", "") for raw_id in raw_ids]

print(f"Found {len(paper_ids)} recent papers.")

metadata_list = []

with open(log_file, "a", encoding="utf-8") as logf:
    for idx, arxiv_id in enumerate(paper_ids, 1):
        pdf_url = f"{base_pdf_url}{arxiv_id}.pdf"
        pdf_filename = os.path.join(pdf_folder, f"{arxiv_id}.pdf")
        abs_url = f"https://arxiv.org/abs/{arxiv_id}"

        # Scrape metadata (title, abstract, authors)
        title, abstract, authors = "", "", ""
        try:
            abs_resp = requests.get(abs_url)
            abs_soup = BeautifulSoup(abs_resp.text, "html.parser")
            title_elem = abs_soup.find("h1", class_="title")
            title = title_elem.text.replace("Title:", "").strip() if title_elem else ""
            abstract_elem = abs_soup.find("blockquote", class_="abstract")
            abstract = abstract_elem.text.replace("Abstract:", "").strip() if abstract_elem else ""
            authors_elem = abs_soup.find("div", class_="authors")
            authors = authors_elem.text.replace("Authors:", "").strip() if authors_elem else ""
        except Exception as e:
            logf.write(f"[{idx}] Metadata error {arxiv_id}: {e}\n")
            print(f"⚠️ Metadata error for {arxiv_id}: {e}")

        if os.path.exists(pdf_filename) and os.path.getsize(pdf_filename) > 0:
            print(f"Already exists: {pdf_filename}")
            logf.write(f"[{idx}] Skipped {arxiv_id} (already exists)\n")
        else:
            try:
                pdf_response = requests.get(pdf_url)
                if pdf_response.status_code == 200:
                    with open(pdf_filename, "wb") as f:
                        f.write(pdf_response.content)
                    print(f"Downloaded: {pdf_filename}")
                    logf.write(f"[{idx}] Downloaded {arxiv_id}\n")
                else:
                    print(f"Failed to download {arxiv_id}: HTTP {pdf_response.status_code}")
                    logf.write(f"[{idx}] Failed to download {arxiv_id}: HTTP {pdf_response.status_code}\n")
                time.sleep(1)
            except Exception as e:
                print(f"Error downloading {arxiv_id}: {e}")
                logf.write(f"[{idx}] Error downloading {arxiv_id}: {e}\n")

        metadata_list.append({
            "arxiv_id": arxiv_id,
            "title": title,
            "abstract": abstract,
            "authors": authors,
            "pdf_path": pdf_filename,
            "arxiv_url": abs_url
        })

# Write/update metadata CSV
with open(metadata_csv, "w", newline='', encoding="utf-8") as csvfile:
    fieldnames = ["arxiv_id", "title", "abstract", "authors", "pdf_path", "arxiv_url"]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(metadata_list)

print(f"\n📄 Metadata saved to: {metadata_csv}")
print(f"📁 PDFs saved to: {pdf_folder}")
print(f"🧾 Download log: {log_file}")
print(f"Total papers processed: {len(paper_ids)}")
print(f"Last updated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
# End of script