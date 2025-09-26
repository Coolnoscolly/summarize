from fastapi import FastAPI, Depends
from pydantic import BaseModel
from typing import List
from app.dependencies import get_summary_pipeline
 
app = FastAPI()

class ListFiles(BaseModel):
    list_files: str
 
@app.post("/summarize")
def summarize(file_list: ListFiles, flow = Depends(get_summary_pipeline)):

    return flow.run(file_list=file_list.list_files) 

if __name__ == '__main__':
    import uvicorn

    uvicorn.run('app.main:app', host = '0.0.0.0', port = 6000)


