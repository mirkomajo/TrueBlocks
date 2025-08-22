from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI(title="TrueBlocks API")

class Block(BaseModel):
    number: int
    data: str

_blocks_db: list[Block] = []

@app.get("/blocks", response_model=list[Block])
def list_blocks() -> list[Block]:
    return _blocks_db

@app.post("/blocks", response_model=Block)
def add_block(block: Block) -> Block:
    _blocks_db.append(block)
    return block
