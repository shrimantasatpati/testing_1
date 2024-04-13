https://celery.school/celery-on-windows
#https://stackoverflow.com/questions/45744992/celery-raises-valueerror-not-enough-values-to-unpack
https://docs.celeryq.dev/en/latest/reference/celery.result.html
https://medium.com/@Aman-tech/celery-with-flask-d1f1c555ceb7
https://celery.school/custom-celery-task-states


"""
    run -
"""
# celery -A celery_chroma_ingest worker --loglevel=INFO --pool=solo

"""
    below ways -
    it is used for multithreading
"""
# celery -A celery_chroma_ingest worker --loglevel=INFO --pool=threads --concurrency=2

https://celery.school/celery-on-windows
# celery -A celery_chroma_ingest worker --loglevel=INFO --pool=gevent --concurrency=8










import os
import datetime
from celery import Celery
from celery.schedules import crontab
import chromadb
from chromadb.config import Settings
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain.vectorstores.chroma import Chroma
from langchain_core.documents.base import Document
import PyPDF2
from redis import Redis
import logging
from celery.result import AsyncResult
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_community.embeddings import HuggingFaceEmbeddings
import docx
import sqlite3
from typing import List
from pdf_utils import check_if_scanned_full_doc, convert_to_doc_intell_pdf_format, process

logger = logging.getLogger(__name__)

# Celery setup
redis = Redis(host="20.41.249.147", port=6379, username="default", password="admin", db=0)
app = Celery('celery_chroma_ingest', broker='redis://20.41.249.147:6379/0', backend="redis://20.41.249.147:6379/0")

# Initialize Chroma DB client
chroma_client = chromadb.PersistentClient(path="chromadb")
collection = chroma_client.get_or_create_collection(name="pdf_database")

def get_text(file_path):
    if file_path.endswith('.pdf'):
        scanned_flag, _ = check_if_scanned_full_doc(file_path)
        if scanned_flag:
            file_names = convert_to_doc_intell_pdf_format(file_path)
            text = ""
            for file_name in file_names:
                docs = process(file_name)
                for doc in docs:
                    text += doc['block'] + "\n"
        else:
            pdf_file = open(file_path, 'rb')
            pdf_reader = PyPDF2.PdfReader(pdf_file)
            text = ""
            for page_num in range(len(pdf_reader.pages)):
                text += pdf_reader.pages[page_num].extract_text()
            pdf_file.close()
    elif file_path.endswith('.txt'):
        with open(file_path, 'r', encoding='utf-8') as file:
            text = file.read()
    elif file_path.endswith('.docx'):
        doc = docx.Document(file_path)
        text = ""
        for para in doc.paragraphs:
            text += para.text + "\n"
    else:
        text = ""
    return text

def get_text_from_database(file_path):
    conn = sqlite3.connect(file_path)
    cursor = conn.cursor()    
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'") # fetching list of tables from sqlite3 database
    tables = [row[0] for row in cursor.fetchall()]

    text = ""

    for table in tables:
        cursor.execute(f"PRAGMA table_info({table})") # fetching list of column names, data types
        column_info = cursor.fetchall()        
        string_columns = [info[1] for info in column_info if info[2] in ('TEXT', 'CLOB')] # names of string type columns

        # Fetch data from string columns
        rows = []
        for column in string_columns:
            cursor.execute(f"SELECT {column} FROM {table}")
            rows.extend([str(row[0]) for row in cursor.fetchall()])

        text += "\n".join(rows) + "\n"

    conn.close()
    return text

# Initialize text splitter and embeddings
text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=100)
embeddings = HuggingFaceEmbeddings(model_name="BAAI/bge-large-en-v1.5")

@app.task
def process_files():
    uploads_dir = 'uploads'
    files_processed = set()
    for filename in os.listdir(uploads_dir):
        if filename.endswith('.pdf') or filename.endswith(".txt") or filename.endswith(".docx") or filename.endswith('.db'):
            file_path = os.path.join(uploads_dir, filename)
            if file_path not in files_processed:

                if filename.endswith('.db'):
                    # Get text from the database
                    text = get_text_from_database(file_path)
                else:
                    text = get_text(file_path)
                chunks = text_splitter.split_text(text)

                # Convert chunks to vector representations and store in Chroma DB
                documents_list = []
                embeddings_list = []
                ids_list = []
                for i, chunk in enumerate(chunks):
                    vector = embeddings.embed_query(chunk)
                    documents_list.append(chunk)
                    embeddings_list.append(vector)
                    ids_list.append(f"{filename}_{i}")
                try:
                    collection.add(embeddings=embeddings_list, documents=documents_list, ids=ids_list)
                except chromadb.errors.ChromaDBError as e:
                    logger.warning(f"Error adding embeddings for {filename}: {e}")
                logger.info(f"Stored {filename} in vector database")
                files_processed.add(file_path)

    print("------")
    print(collection)
    task_id = process_files.request.id
    return task_id

# Register the task with the Celery app
app.tasks.register(process_files)

if __name__ == "__main__":
    app.start()