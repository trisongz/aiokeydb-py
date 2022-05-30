"""
Module containing the main config classes

Modified from https://github.com/andrewthetechie/pydantic-aioredis/blob/main/pydantic_aioredis/config.py
"""
from typing import Optional

from pydantic import BaseModel


class KeyDBConfig(BaseModel):
    """A config object for connecting to keydb"""

    host: str = "localhost"
    port: int = 6379
    db: int = 0
    username: Optional[str] = None
    password: Optional[str] = None
    ssl: bool = False
    encoding: Optional[str] = "utf-8"

    @property
    def keydb_url(self) -> str:
        """Returns a keydb url to connect to"""
        proto = "keydbs" if self.ssl else "keydb"
        if self.password is None:
            return f"{proto}://{self.host}:{self.port}/{self.db}"
        if self.username:
            return f"{proto}://{self.username}:{self.password}@{self.host}:{self.port}/{self.db}"
        return f"{proto}://:{self.password}@{self.host}:{self.port}/{self.db}"

    class Config:
        """Pydantic schema config"""

        orm_mode = True