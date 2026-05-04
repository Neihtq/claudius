from abc import ABC, abstractmethod
from claudius.models import Session, ToolMount


class AbstractPlugin(ABC):
    @abstractmethod
    async def setup(self, session: Session, config: dict) -> ToolMount:
        """Provision credentials and return what to inject into the execution."""

    @abstractmethod
    async def teardown(self, session: Session) -> None:
        """Revoke credentials and clean up resources."""
