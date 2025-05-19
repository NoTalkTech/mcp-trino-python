from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List


class QueryRequest(BaseModel):
    query: str = Field(..., description="SQL query to execute")
    params: Optional[Dict[str, Any]] = Field(default=None, description="Query parameters")
    limit: Optional[int] = Field(default=None, description="Limit the number of results")


class QueryResponse(BaseModel):
    columns: List[str]
    data: List[List[Any]]
    row_count: int


class ErrorResponse(BaseModel):
    error: str
    detail: Optional[str] = None 