import os
import fitz  # PyMuPDF

pdf_dir = "data/scraped_arxiv_pdfs/pdfs"
text_dir = "data/scraped_arxiv_pdfs/texts"
os.makedirs(text_dir, exist_ok=True)

for pdf_file in os.listdir(pdf_dir):
    if not pdf_file.endswith(".pdf"):
        continue
    pdf_path = os.path.join(pdf_dir, pdf_file)
    txt_path = os.path.join(text_dir, pdf_file.replace(".pdf", ".txt"))
    if os.path.exists(txt_path) and os.path.getsize(txt_path) > 10:
        print(f"Already extracted: {txt_path}")
        continue
    try:
        doc = fitz.open(pdf_path)
        text = ""
        for page in doc:
            text += page.get_text()
        with open(txt_path, "w", encoding="utf-8") as f:
            f.write(text)
        print(f"Extracted: {txt_path}")
    except Exception as e:
        print(f"Error extracting {pdf_file}: {e}")
# Clean up
for pdf_file in os.listdir(pdf_dir):
    if not pdf_file.endswith(".pdf"):
        continue
    pdf_path = os.path.join(pdf_dir, pdf_file)
    txt_path = os.path.join(text_dir, pdf_file.replace(".pdf", ".txt"))
    if not os.path.exists(txt_path) or os.path.getsize(txt_path) < 10:
        os.remove(pdf_path)
        print(f"Removed empty or failed extraction: {pdf_file}")
# This script extracts text from PDF files in a specified directory and saves the text to a new directory.
# It skips files that have already been processed and removes any PDFs that failed to extract text.
# This script extracts text from PDF files in a specified directory and saves the text to a new directory.
# It skips files that have already been processed and removes any PDFs that failed to extract text.