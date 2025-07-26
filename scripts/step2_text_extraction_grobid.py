import os
import requests
from tqdm import tqdm

# GROBID server endpoint
GROBID_URL = "http://localhost:8070/api/processFulltextDocument"

def extract_pdf(pdf_path, out_xml_path):
    """
    Send PDF to GROBID server and save the TEI XML result.
    """
    with open(pdf_path, 'rb') as pdf_file:
        files = {'input': (os.path.basename(pdf_path), pdf_file, 'application/pdf')}
        response = requests.post(GROBID_URL, files=files)
        if response.status_code == 200:
            with open(out_xml_path, 'w', encoding='utf-8') as out_file:
                out_file.write(response.text)
            print(f"Extracted: {pdf_path} -> {out_xml_path}")
        else:
            print(f"Failed: {pdf_path} (status {response.status_code})")

def batch_extract_pdf(input_folder, output_folder):
    """
    Process all PDFs in input_folder, extracting to output_folder.
    Skips PDFs that already have a TEI XML output.
    """
    os.makedirs(output_folder, exist_ok=True)
    pdf_files = [f for f in os.listdir(input_folder) if f.lower().endswith('.pdf')]
    for pdf_name in tqdm(pdf_files, desc="Processing PDFs"):
        pdf_path = os.path.join(input_folder, pdf_name)
        out_xml_path = os.path.join(output_folder, pdf_name.replace('.pdf', '.tei.xml'))

        # Skip if the output file already exists
        if os.path.exists(out_xml_path):
            print(f"Skipped (already exists): {out_xml_path}")
            continue

        extract_pdf(pdf_path, out_xml_path)

if __name__ == "__main__":
    # ==== Set your folders here ====
    input_folder = r"C:\Users\doshi\SEAL Project\data\scraped_arxiv_pdfs\pdfs"     # Folder with PDFs
    output_folder = r"C:\Users\doshi\SEAL Project\data\scraped_arxiv_pdfs\TEI_XMLs"   # Where TEI XML will be saved

    batch_extract_pdf(input_folder, output_folder)
# This script extracts text from PDF files using GROBID and saves the output in TEI XML format.
# It processes all PDFs in a specified input folder and saves the results in an output folder.
# It skips files that have already been processed.
