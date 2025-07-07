import logging
from typing import Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)

class TaskHandler:
    """
    Base class for all task handlers.
    """
    async def handle(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError("Subclasses must implement handle method")


class WebScraperTaskHandler(TaskHandler):
    """
    Handler for web scraping tasks.
    """
    async def handle(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        try:
            url = parameters.get("url", "")
            
            # simple concatenate parameters as result
            result = f"Web scraper processed URL: {url}"
            
            return {
                "input_parameters": parameters,
                "result": result,
                "processed_at": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Web scraper failed: {parameters}, error: {str(e)}")
            raise
        

class CalculatorTaskHandler(TaskHandler):
    """
    Handler for calculator tasks.
    """
    async def handle(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        try:
            a = parameters.get("a", 0)
            b = parameters.get("b", 0)
            operation = parameters.get("operation", "add")
            
            result = f"Calculator processed: {a} {operation} {b}"
            
            return {
                "input_parameters": parameters,
                "result": result,
                "processed_at": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Calculator task failed: {parameters}, error: {str(e)}")
            raise

class TaskHandlerFactory:
    """
    Factory for creating task handlers.
    """
    _handlers = {
        "web_scraper": WebScraperTaskHandler,
        "calculator": CalculatorTaskHandler
    }

    @classmethod
    def get_handler(cls, task_type: str) -> TaskHandler:
        handler_class = cls._handlers.get(task_type)
        if not handler_class:
            raise ValueError(f"No handler found for task type: {task_type}")
        return handler_class()

    @classmethod
    def register_handler(cls, task_type: str, handler_class):
        """register new task handler"""
        cls._handlers[task_type] = handler_class
    
    @classmethod
    def get_supported_types(cls) -> list:
        """get supported task types"""
        return list(cls._handlers.keys())







    