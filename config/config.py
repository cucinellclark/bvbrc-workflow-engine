"""Configuration management for workflow engine."""
import os
import yaml
from typing import Dict, Any, Optional
from pathlib import Path


class Config:
    """Configuration loader and manager."""
    
    _instance: Optional['Config'] = None
    _config: Dict[str, Any] = {}
    
    def __new__(cls):
        """Singleton pattern to ensure single config instance."""
        if cls._instance is None:
            cls._instance = super(Config, cls).__new__(cls)
        return cls._instance
    
    def load(self, config_path: str = None):
        """Load configuration from YAML file.
        
        Args:
            config_path: Path to config file. If None, uses default location.
        """
        if config_path is None:
            # Default to config.yaml in the config directory
            config_dir = Path(__file__).parent
            config_path = config_dir / "config.yaml"
        
        with open(config_path, 'r') as f:
            self._config = yaml.safe_load(f)
        
        # Override with environment variables if present
        self._override_from_env()
        
        return self
    
    def _override_from_env(self):
        """Override config values with environment variables."""
        # MongoDB overrides
        if os.getenv('MONGODB_HOST'):
            self._config['mongodb']['host'] = os.getenv('MONGODB_HOST')
        if os.getenv('MONGODB_PORT'):
            self._config['mongodb']['port'] = int(os.getenv('MONGODB_PORT'))
        if os.getenv('MONGODB_DATABASE'):
            self._config['mongodb']['database'] = os.getenv('MONGODB_DATABASE')
        if os.getenv('MONGODB_USERNAME'):
            self._config['mongodb']['username'] = os.getenv('MONGODB_USERNAME')
        if os.getenv('MONGODB_PASSWORD'):
            self._config['mongodb']['password'] = os.getenv('MONGODB_PASSWORD')
        
        # API overrides
        if os.getenv('API_HOST'):
            self._config['api']['host'] = os.getenv('API_HOST')
        if os.getenv('API_PORT'):
            self._config['api']['port'] = int(os.getenv('API_PORT'))
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value by dot-notation key.
        
        Args:
            key: Configuration key (e.g., 'mongodb.host')
            default: Default value if key not found
            
        Returns:
            Configuration value
        """
        keys = key.split('.')
        value = self._config
        
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
            else:
                return default
            
            if value is None:
                return default
        
        return value
    
    @property
    def mongodb(self) -> Dict[str, Any]:
        """Get MongoDB configuration."""
        return self._config.get('mongodb', {})
    
    @property
    def api(self) -> Dict[str, Any]:
        """Get API configuration."""
        return self._config.get('api', {})
    
    @property
    def scheduler(self) -> Dict[str, Any]:
        """Get scheduler configuration."""
        return self._config.get('scheduler', {})
    
    @property
    def logging(self) -> Dict[str, Any]:
        """Get logging configuration."""
        return self._config.get('logging', {})


# Global config instance
config = Config()

