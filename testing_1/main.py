from fastapi import FastAPI, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from controller import upload_files, get_responses

app = FastAPI()

# origins = [
#     "http://localhost",
#     "http://localhost:3000",
#     "http://localhost:8000"
# ]
app.add_middleware(
    CORSMiddleware,
    # allow_origins=origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

@app.post("/data_ingestion")
async def upload_files_route(files: list[UploadFile] = File(...)):
    return await upload_files(files)

@app.get("/responses")
def get_responses_route():
    return get_responses()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)    