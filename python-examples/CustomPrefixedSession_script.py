import requests
from urllib.parse import urljoin, urlparse
from typing import Optional, Dict, Any, Union
import logging

class CustomPrefixedSession(requests.Session):
    def __init__(
        self,
        base_url: Optional[str] = None,
        default_timeout: Optional[Union[float, tuple]] = None,
        default_headers: Optional[Dict[str, str]] = None,
        api_key: Optional[str] = None,
        api_key_header: str = 'Authorization',
        api_key_prefix: str = 'Bearer',
        verify_ssl: bool = True,
        debug: bool = False
    ):
        super().__init__()
        
        # Configure base URL
        self.base_url = None
        if base_url:
            self.set_base_url(base_url)
        
        # Set default timeout
        self.default_timeout = default_timeout
        
        # Configure SSL verification
        self.verify = verify_ssl
        
        # Set up logging
        self.debug = debug
        if debug:
            self.logger = logging.getLogger(self.__class__.__name__)
            self.logger.setLevel(logging.DEBUG)
        
        # Set default headers
        if default_headers:
            self.headers.update(default_headers)
        
        # Set up API key authentication
        if api_key:
            self.set_api_key(api_key, api_key_header, api_key_prefix)
    
    def set_base_url(self, base_url: str):
        """Set or update the base URL"""
        if not base_url:
            self.base_url = None
            return
        
        # Ensure base_url ends with /
        self.base_url = base_url.rstrip('/') + '/'
        
        # Validate it's a proper URL
        parsed = urlparse(self.base_url)
        if not parsed.scheme or not parsed.netloc:
            raise ValueError(f"Invalid base URL: {base_url}")
        
        if self.debug:
            self.logger.debug(f"Base URL set to: {self.base_url}")
    
    def set_api_key(self, api_key: str, header: str = 'Authorization', prefix: str = 'Bearer'):
        """Set API key for authentication"""
        if prefix:
            auth_value = f"{prefix} {api_key}"
        else:
            auth_value = api_key
        
        self.headers[header] = auth_value
        
        if self.debug:
            self.logger.debug(f"API key set in header: {header}")
    
    def request(self, method: str, url: str, **kwargs) -> requests.Response:
        """Override request to add URL prefixing and default settings"""
        original_url = url
        
        # Add base URL if configured and URL is relative
        if self.base_url and not self._is_absolute_url(url):
            url = urljoin(self.base_url, url.lstrip('/'))
        
        # Set default timeout if not provided
        if 'timeout' not in kwargs and self.default_timeout:
            kwargs['timeout'] = self.default_timeout
        
        if self.debug:
            self.logger.debug(f"{method.upper()} {original_url} -> {url}")
        
        try:
            response = super().request(method, url, **kwargs)
            
            if self.debug:
                self.logger.debug(f"Response: {response.status_code} {response.reason}")
            
            return response
        
        except requests.RequestException as e:
            if self.debug:
                self.logger.error(f"Request failed: {e}")
            raise
    
    def _is_absolute_url(self, url: str) -> bool:
        """Check if URL is absolute"""
        return bool(urlparse(url).scheme)
    
    # Convenience methods for common HTTP methods with JSON support
    def get_json(self, url: str, **kwargs) -> Dict[str, Any]:
        """GET request that returns JSON response"""
        response = self.get(url, **kwargs)
        response.raise_for_status()
        return response.json()
    
    def post_json(self, url: str, data: Optional[Dict[str, Any]] = None, **kwargs) -> requests.Response:
        """POST request with JSON data"""
        if data is not None:
            kwargs['json'] = data
        return self.post(url, **kwargs)
    
    def put_json(self, url: str, data: Optional[Dict[str, Any]] = None, **kwargs) -> requests.Response:
        """PUT request with JSON data"""
        if data is not None:
            kwargs['json'] = data
        return self.put(url, **kwargs)
    
    def patch_json(self, url: str, data: Optional[Dict[str, Any]] = None, **kwargs) -> requests.Response:
        """PATCH request with JSON data"""
        if data is not None:
            kwargs['json'] = data
        return self.patch(url, **kwargs)
    
    # API-specific convenience methods
    def api_call(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        """Generic API call method"""
        return self.request(method, endpoint, **kwargs)
    
    def paginated_get(self, url: str, page_param: str = 'page', 
                     start_page: int = 1, max_pages: Optional[int] = None, **kwargs):
        """Generator for paginated GET requests"""
        page = start_page
        pages_fetched = 0
        
        while True:
            # Add page parameter
            params = kwargs.get('params', {}).copy()
            params[page_param] = page
            kwargs['params'] = params
            
            response = self.get(url, **kwargs)
            response.raise_for_status()
            
            yield response
            
            pages_fetched += 1
            if max_pages and pages_fetched >= max_pages:
                break
            
            # Simple check for empty response (customize based on your API)
            try:
                data = response.json()
                if not data or (isinstance(data, list) and len(data) == 0):
                    break
            except ValueError:
                break
            
            page += 1

# Usage examples
session = CustomPrefixedSession(
    base_url='https://api.github.com',
    default_timeout=30,
    default_headers={'Accept': 'application/vnd.github.v3+json'},
    api_key='ghp_your_token_here',
    debug=True
)

# Basic requests
user = session.get_json('user')  # GET https://api.github.com/user
repos = session.get_json('user/repos')  # GET https://api.github.com/user/repos

# Create a new repo
new_repo_data = {'name': 'test-repo', 'private': False}
response = session.post_json('user/repos', new_repo_data)

# Paginated requests
for page_response in session.paginated_get('user/repos', max_pages=3):
    repos = page_response.json()
    print(f"Got {len(repos)} repositories")
    
    
