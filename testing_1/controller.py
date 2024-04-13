import os
from celery_chroma_ingest import process_files, collection
from celery.result import AsyncResult
from task_database import store_responses, get_responses_from_db
from fastapi import FastAPI, File, UploadFile

async def upload_files(files: list[UploadFile]):
    uploads_dir = 'uploads'
    uploads_dir_path = os.path.abspath(uploads_dir)
    # Create uploads dir if not exists
    if not os.path.exists(uploads_dir):
        os.makedirs(uploads_dir)

    responses = []
    for file in files:
        file_path = os.path.join(uploads_dir, file.filename)
        with open(file_path, "wb") as buffer:
            buffer.write(file.file.read())

        # Trigger the Celery task to process the new file
        task = process_files.delay()
        task_id = task.id
        async_result = AsyncResult(task_id, app=process_files)
        
        # Wait for previous task completion before triggering the next one
        async_result.get()
        task_status = async_result.status
        task_time = async_result.date_done
        # task_status = task_status.update_state
        
        response = {
            "task_id": task_id,
            "status": task_status,
            "time": task_time if task_time else None,
            "collection_name": collection.name,
            "uploads_dir": uploads_dir_path,
            "file_name": file.filename,
            # "result": async_result.result,
            # "successful": async_result.successful(),
        }
        responses.append(response)

    store_responses(responses)
    return responses

def get_responses():
    return get_responses_from_db()