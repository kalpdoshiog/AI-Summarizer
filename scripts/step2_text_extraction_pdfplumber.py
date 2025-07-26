import pdfplumber
import pandas as pd
import os
from glob import glob
from tqdm import tqdm

pdf_folder = r"C:\Users\doshi\SEAL Project\data\scraped_arxiv_pdfs\pdfs"
pdf_files = glob(os.path.join(pdf_folder, "*.pdf"))

output_text_folder = r"C:\Users\doshi\SEAL Project\data\scraped_arxiv_pdfs\pdf_plumber_extracted_texts"
output_table_folder = r"C:\Users\doshi\SEAL Project\data\scraped_arxiv_pdfs\pdf_plumber_extracted_tables"
output_image_folder = r"C:\Users\doshi\SEAL Project\data\scraped_arxiv_pdfs\pdf_plumber_extracted_images"
os.makedirs(output_text_folder, exist_ok=True)
os.makedirs(output_table_folder, exist_ok=True)
os.makedirs(output_image_folder, exist_ok=True)

error_log_path = r"C:\Users\doshi\SEAL Project\data\scraped_arxiv_pdfs\logs\pdfplumber_errors.log"
os.makedirs(os.path.dirname(error_log_path), exist_ok=True)

with open(error_log_path, "w", encoding="utf-8") as log_file:
    for pdf_path in tqdm(pdf_files, desc="PDFs Processed", ncols=90):
        pdf_name = os.path.splitext(os.path.basename(pdf_path))[0]
        print(f"\nProcessing: {pdf_name}")

        text_file = os.path.join(output_text_folder, f"{pdf_name}.txt")
        if os.path.exists(text_file):
            print(f"  Text exists, skipping: {text_file}")
            continue

        all_text = ""
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for i, page in enumerate(pdf.pages):
                    # Table Extraction
                    try:
                        tables = page.extract_tables()
                        for t_idx, table in enumerate(tables):
                            table_csv = os.path.join(
                                output_table_folder, f"{pdf_name}_page{i+1}_table{t_idx+1}.csv"
                            )
                            if not os.path.exists(table_csv):
                                df = pd.DataFrame(table)
                                df.to_csv(table_csv, index=False)
                                print(f"  Table saved: {table_csv}")
                            else:
                                print(f"  Table exists, skipping: {table_csv}")
                    except Exception as e:
                        log_file.write(f"{pdf_name} page {i+1} (table): {repr(e)}\n")

                    # Image Extraction
                    try:
                        images = page.images
                        for img_idx, img_dict in enumerate(images):
                            image_path = os.path.join(
                                output_image_folder, f"{pdf_name}_page{i+1}_img{img_idx+1}.png"
                            )
                            if os.path.exists(image_path):
                                print(f"  Image exists, skipping: {image_path}")
                                continue
                            cropped = page.crop((img_dict["x0"], img_dict["top"], img_dict["x1"], img_dict["bottom"]))
                            pil_image = cropped.to_image(resolution=300).original
                            pil_image.save(image_path)
                            print(f"  Image saved: {image_path}")
                    except Exception as e:
                        log_file.write(f"{pdf_name} page {i+1} (image): {repr(e)}\n")

                    # Text Extraction
                    try:
                        text = page.extract_text()
                        if text:
                            all_text += f"\n--- Page {i+1} ---\n{text}"
                    except Exception as e:
                        log_file.write(f"{pdf_name} page {i+1} (text): {repr(e)}\n")
        except Exception as e:
            log_file.write(f"{pdf_name} (open): {repr(e)}\n")
            print(f"  [ERROR] Could not open {pdf_name}, skipping.")
            continue
        # Save text even if partial
        with open(text_file, "w", encoding="utf-8") as f:
            f.write(all_text)
        print(f"  Text saved: {text_file}")

print("\nBatch extraction completed. Check 'pdfplumber_errors.log' for skipped pages/files.")
