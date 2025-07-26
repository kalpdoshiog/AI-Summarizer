import pdfplumber
import fitz
import pandas as pd
import os
from glob import glob
from paddleocr import PaddleOCR
from tqdm import tqdm
import traceback
import datetime

# CONFIG
pdf_folder = r"C:\Users\doshi\SEAL Project\data\scraped_arxiv_pdfs\pdfs"
output_text_folder = r"C:\Users\doshi\SEAL Project\data\scraped_arxiv_pdfs\hybrid_texts"
output_table_folder = r"C:\Users\doshi\SEAL Project\data\scraped_arxiv_pdfs\hybrid_tables"
output_image_folder = r"C:\Users\doshi\SEAL Project\data\scraped_arxiv_pdfs\hybrid_images"
output_ocr_image_folder = r"C:\Users\doshi\SEAL Project\data\scraped_arxiv_pdfs\hybrid_page_images"
error_log_path = r"C:\Users\doshi\SEAL Project\data\scraped_arxiv_pdfs\logs\hybrid_extractor_errors.log"

for folder in [
    output_text_folder, output_table_folder, output_image_folder, output_ocr_image_folder,
    os.path.dirname(error_log_path)
]:
    os.makedirs(folder, exist_ok=True)

ocr = PaddleOCR(lang='en', device="gpu:0")
pdf_files = glob(os.path.join(pdf_folder, "*.pdf"))

def log_error(msg):
    with open(error_log_path, "a", encoding="utf-8") as elog:
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        elog.write(f"[{timestamp}] {msg}\n")

with tqdm(total=len(pdf_files), desc="PDFs Total Progress", ncols=110) as total_bar:
    for pdf_path in pdf_files:
        pdf_name = os.path.splitext(os.path.basename(pdf_path))[0]
        text_file = os.path.join(output_text_folder, f"{pdf_name}.txt")

        if os.path.exists(text_file):
            total_bar.update(1)
            continue

        all_text = ""
        ocr_fallback = False

        try:
            with pdfplumber.open(pdf_path) as pdf:
                for i, page in enumerate(tqdm(pdf.pages, desc=f"{pdf_name} Pages", leave=False, ncols=80)):
                    # Tables
                    try:
                        tables = page.extract_tables()
                        for t_idx, table in enumerate(tqdm(tables, desc=f"Tables p{i+1}", leave=False, ncols=65)):
                            table_csv = os.path.join(output_table_folder, f"{pdf_name}_page{i+1}_table{t_idx+1}.csv")
                            if not os.path.exists(table_csv):
                                pd.DataFrame(table).to_csv(table_csv, index=False)
                    except Exception as e:
                        log_error(f"[Table Error] {pdf_name} page {i+1}: {repr(e)}\n{traceback.format_exc()}")
                    # Images
                    try:
                        images = page.images
                        for img_idx, img in enumerate(tqdm(images, desc=f"Images p{i+1}", leave=False, ncols=65)):
                            img_path = os.path.join(output_image_folder, f"{pdf_name}_page{i+1}_img{img_idx+1}.png")
                            if os.path.exists(img_path):
                                continue
                            crop = page.crop((img["x0"], img["top"], img["x1"], img["bottom"]))
                            crop.to_image(resolution=300).original.save(img_path)
                    except Exception as e:
                        log_error(f"[Image Error] {pdf_name} page {i+1}: {repr(e)}\n{traceback.format_exc()}")
                    # Text
                    try:
                        text = page.extract_text()
                        if text and len(text.strip()) > 10:
                            all_text += f"\n--- Page {i+1} ---\n{text}"
                        else:
                            ocr_fallback = True
                    except Exception as e:
                        ocr_fallback = True
                        log_error(f"[Text Error] {pdf_name} page {i+1}: {repr(e)}\n{traceback.format_exc()}")
        except Exception as e:
            ocr_fallback = True
            log_error(f"[Open Error] {pdf_name}: {repr(e)}\n{traceback.format_exc()}")

        # OCR fallback using GPU
        if ocr_fallback or not all_text.strip():
            try:
                doc = fitz.open(pdf_path)
                for pnum in tqdm(range(doc.page_count), desc=f"{pdf_name} OCR Pages", leave=False, ncols=75):
                    page = doc.load_page(pnum)
                    img_path = os.path.join(output_ocr_image_folder, f"{pdf_name}_page{pnum+1}.png")
                    if not os.path.exists(img_path):
                        page.get_pixmap(dpi=300).save(img_path)
                    try:
                        res = ocr.ocr(img_path)
                        for line in res[0]:
                            all_text += line[1][0] + "\n"
                    except Exception as e:
                        log_error(f"[OCR Error] {pdf_name} page {pnum+1}: {repr(e)}\n{traceback.format_exc()}")
            except Exception as e:
                log_error(f"[OCR Fallback Error] {pdf_name}: {repr(e)}\n{traceback.format_exc()}")

        try:
            with open(text_file, "w", encoding="utf-8") as f:
                f.write(all_text)
        except Exception as e:
            log_error(f"[Save Text Error] {pdf_name}: {repr(e)}\n{traceback.format_exc()}")

        total_bar.update(1)

print("\n✅ Extraction complete! Check log file for errors.")
if __name__ == "__main__":
    print("Batch PDF extraction completed.")
# This script extracts text, tables, and images from PDF files using pdfplumber and PaddleOCR with GPU support.
# It handles errors gracefully and logs them for review.
# It also performs OCR on pages where text extraction fails or is insufficient.
# The output is saved in specified folders for text, tables, images, and OCR images.
# The script uses tqdm for progress tracking and fitz for image extraction.
# Ensure you have the required libraries installed: pdfplumber, paddleocr, fitz, pandas, tqdm.
# Adjust the paths in the CONFIG section as needed for your environment.
# Note: This script requires a GPU with PaddleOCR configured to use it.
# Make sure to run this script in an environment where PaddleOCR and pdfplumber are properly installed.
