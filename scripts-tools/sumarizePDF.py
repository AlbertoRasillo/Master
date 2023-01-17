#!/usr/bin/python

import os
import openai
import textwrap
import fitz
from typing import List, Tuple

def _parse_highlight(annot: fitz.Annot, wordlist: List[Tuple[float, float, float, float, str, int, int, int]]) -> str:
    points = annot.vertices
    quad_count = int(len(points) / 4)
    sentences = []
    for i in range(quad_count):
        # where the highlighted part is
        r = fitz.Quad(points[i * 4 : i * 4 + 4]).rect

        words = [w for w in wordlist if fitz.Rect(w[:4]).intersects(r)]
        sentences.append(" ".join(w[4] for w in words))
    sentence = " ".join(sentences)
    return sentence

def handle_page(page):
    wordlist = page.get_text("words")  # list of words on page
    wordlist.sort(key=lambda w: (w[3], w[0]))  # ascending y, then x

    highlights = []
    annot = page.first_annot
    while annot:
        if annot.type[0] == 8:
            highlights.append(_parse_highlight(annot, wordlist))
        annot = annot.next
    return highlights

def convert_pdf_to_txt(filepath: str) -> List:
    doc = fitz.open(filepath)

    highlights = []
    for page in doc:
        highlights += handle_page(page)

    return highlights

def split_text_multiple_string_to_file(text, output_dir):
    text = "\n".join(text)
    lines = text.splitlines()
    chunks = [lines[i:i+2000] for i in range(0, len(lines), 2000)]

    for i, chunk in enumerate(chunks):
        with open(f'{output_dir}/part{i}.txt', 'w') as f:
            f.write("\n".join(chunk))

def split_text_one_string_to_file(text, output_dir):
    # divide el texto en ficheros de 2500 palabras
    chunks = textwrap.wrap(text, 2000)
    # guardar cada trozo de texto en un fichero de texto
    for i, chunk in enumerate(chunks):
        with open(f'{output_dir}/part{i}.txt', 'w') as f:
            f.write(chunk)

def convert_text_files_to_markdown(input_dir, output_file):
    markdown = ""
    # Recorre todos los ficheros de texto en el directorio de entrada
    for file in os.listdir(input_dir):
        if file.endswith(".txt"):
            file_path = os.path.join(input_dir, file)
            with open(file_path, 'r') as f:
                text = f.read()
                request = "converts the following text to markdown format identifying and adding: \
                    - titles\
                    - subtitles\
                    - lists\
                    - paragraphs\
                    - punctuation marks\
                    - symbols\
                    - everything necessary to facilitate reading: \\"
                prompt = f"{request}{text}"
                response = openai.Completion.create(engine="text-davinci-002", prompt=prompt, max_tokens=1024, n=1,stop=None,temperature=0.5)
                markdown += response.choices[0].text
    # Escribir el resultado en el archivo de salida
    with open(output_file, 'w') as f:
        f.write(markdown)

def summarize_text_files(input_dir):
    summary = ""
    # recorre todos los ficheros de texto en el directorio de entrada
    for file in os.listdir(input_dir):
        if file.endswith(".txt"):
            file_path = os.path.join(input_dir, file)
            with open(file_path, 'r') as f:
                text = f.read()
                response = openai.Completion.create(engine="davinci", prompt=text, max_tokens=1024, n=1, stop=None, temperature=0.5)
                summary += response.choices[0].text
    return summary

def main():
   
    openai.api_key = ""
    input_dir = "entradaPdf"
    output_dir = "salidaPdf"

    # crear el directorio de salida si no existe
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # recorre todos los ficheros pdf en el directorio de entrada
    for file in os.listdir(input_dir):
        if file.endswith(".pdf"):
            file_path = os.path.join(input_dir, file)
            pdf_text = convert_pdf_to_txt(file_path)
            split_text_multiple_string_to_file(pdf_text, output_dir)
    convert_text_files_to_markdown(output_dir, "salida.txt")
    #summary = summarize_text_files(output_dir)
    # guarda el resumen en un fichero de texto
    #with open("resumen.txt", "w") as f:
    #    f.write(summary)

if __name__ == '__main__':
    main()

