import fitz  # PyMuPDF
from PIL import Image
import io
import os
from typing import List

class SlidedeckParser:
    """
    Handles PDF slidedecks by converting pages to images for Multi-Modal LLM analysis.
    Uses PyMuPDF (fitz) for high-performance rendering.
    """
    def __init__(self, output_dir: str = "data/processed/slides"):
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def pdf_to_images(self, pdf_path: str, dpi: int = 150) -> List[str]:
        """
        Converts each page of a PDF into an image file.
        Returns a list of paths to the generated images.
        """
        doc = fitz.open(pdf_path)
        image_paths = []
        
        base_name = os.path.basename(pdf_path).replace(".pdf", "")
        
        for page_index in range(len(doc)):
            page = doc.load_page(page_index)
            pix = page.get_pixmap(matrix=fitz.Matrix(dpi/72, dpi/72))
            
            output_path = os.path.join(self.output_dir, f"{base_name}_page_{page_index}.png")
            pix.save(output_path)
            image_paths.append(output_path)
            
        doc.close()
        return image_paths

    def get_image_bytes(self, image_path: str) -> bytes:
        """
        Utility to read image bytes for LLM ingestion.
        """
        with open(image_path, "rb") as f:
            return f.read()

if __name__ == "__main__":
    print("Slidedeck Parser with PyMuPDF initialized.")
