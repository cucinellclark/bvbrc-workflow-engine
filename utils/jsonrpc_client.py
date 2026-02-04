"""JSON-RPC client for submitting jobs to scheduler apps."""
import json
import uuid
from typing import Dict, Any, Optional, Union, List
import requests
from utils.logger import get_logger


logger = get_logger(__name__)


class JSONRPCClient:
    """Client for making JSON-RPC 2.0 requests to scheduler services."""
    
    def __init__(self, base_url: str, timeout: int = 30, auth_token: Optional[str] = None):
        """Initialize JSON-RPC client.
        
        Args:
            base_url: Base URL of the scheduler service
            timeout: Request timeout in seconds
            auth_token: Optional authorization token for API requests
        """
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self.auth_token = auth_token
        logger.debug(f"JSON-RPC client initialized: base_url={base_url}, timeout={timeout}")
    
    def call(
        self,
        method: str,
        params: Union[Dict[str, Any], List[Any]],
        request_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Make a JSON-RPC 2.0 method call.
        
        Args:
            method: RPC method name (e.g., "AppService.submit2")
            params: Method parameters (can be dict or list)
            request_id: Optional request ID (auto-generated if not provided)
            
        Returns:
            Response result dictionary
            
        Raises:
            requests.exceptions.RequestException: If HTTP request fails
            ValueError: If JSON-RPC response contains an error
        """
        if request_id is None:
            request_id = str(uuid.uuid4())
        
        # Construct JSON-RPC 2.0 request
        payload = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": request_id
        }
        
        # Log params appropriately based on type
        if isinstance(params, dict):
            params_info = f"params_keys={list(params.keys())}"
        elif isinstance(params, list):
            params_info = f"params_length={len(params)}"
        else:
            params_info = f"params_type={type(params)}"
        
        logger.debug(
            f"Making JSON-RPC call: method={method}, "
            f"request_id={request_id}, {params_info}"
        )

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        
        # Add authorization header if token is provided
        if self.auth_token:
            headers['Authorization'] = self.auth_token
        
        try:
            # Log request summary
            logger.info(f"JSON-RPC request: method={method}, request_id={request_id}")
            
            # Log the full request payload at DEBUG level
            logger.debug(
                f"Full JSON-RPC request to {self.base_url}:\n"
                f"  Payload: {json.dumps(payload, indent=2)}"
            )
            
            response = requests.post(
                self.base_url,
                json=payload,
                headers=headers,
                timeout=self.timeout
            )
            
            # Log response status
            logger.debug(f"Response status: {response.status_code}")
            
            # Try to get response body for error reporting
            response_text = response.text
            
            # Check HTTP status code
            if not response.ok:
                logger.error(
                    f"HTTP error {response.status_code} from {self.base_url}:\n"
                    f"  Request method: {method}\n"
                    f"  Request params: {json.dumps(params, indent=2)}\n"
                    f"  Response body: {response_text}"
                )
                response.raise_for_status()
            
            # Parse JSON-RPC response
            try:
                result = response.json()
            except json.JSONDecodeError as e:
                logger.error(
                    f"Failed to parse JSON response from {self.base_url}:\n"
                    f"  Response text: {response_text}\n"
                    f"  Parse error: {e}"
                )
                raise ValueError(f"Invalid JSON response: {e}")
            
            # Log the full JSON-RPC response envelope at DEBUG level
            logger.debug(
                f"Full JSON-RPC response:\n"
                f"  Response: {json.dumps(result, indent=2)}"
            )
            
            # Check for JSON-RPC error
            if "error" in result:
                error = result["error"]
                error_msg = (
                    f"JSON-RPC error from {method}:\n"
                    f"  Code: {error.get('code')}\n"
                    f"  Message: {error.get('message')}\n"
                )
                if "data" in error:
                    error_msg += f"  Data: {json.dumps(error.get('data'), indent=2)}\n"
                error_msg += f"  Request params: {json.dumps(params, indent=2)}"
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            # Return result
            if "result" in result:
                logger.info(f"JSON-RPC call successful: method={method}")
                return result["result"]
            else:
                logger.warning(
                    f"JSON-RPC response missing 'result' field: {result}"
                )
                return result
                
        except requests.exceptions.Timeout:
            logger.error(
                f"JSON-RPC call timed out:\n"
                f"  Method: {method}\n"
                f"  URL: {self.base_url}\n"
                f"  Timeout: {self.timeout}s\n"
                f"  Request params: {json.dumps(params, indent=2)}"
            )
            raise
        except requests.exceptions.RequestException as e:
            logger.error(
                f"JSON-RPC request failed:\n"
                f"  Method: {method}\n"
                f"  URL: {self.base_url}\n"
                f"  Error: {e}\n"
                f"  Request params: {json.dumps(params, indent=2)}"
            )
            raise
    
    def submit_job(
        self,
        app: str,
        params: Dict[str, Any]
    ) -> str:
        """Submit a job to a specific app via JSON-RPC.
        
        This is a convenience method that calls AppService.submit2
        with the app name and parameters, then extracts the task_id from the response.
        
        The JSON-RPC call format is:
        {
          "jsonrpc": "2.0",
          "method": "AppService.submit2",
          "params": [app_name, step_params, {}],
          "id": request_id
        }
        
        Args:
            app: Application name (e.g., "Assembly2", "Annotation", "ComparativeSystems")
            params: Job parameters to pass to the app (step params dictionary)
            
        Returns:
            Task ID string returned by the scheduler
            
        Raises:
            requests.exceptions.RequestException: If HTTP request fails
            ValueError: If JSON-RPC response contains an error or missing task_id
        """
        # Use AppService.start_app2 method with params as array: [app_name, step_params, {}]
        method = "AppService.start_app2"
        
        # Construct params array: [app_name, step_params, {}]
        rpc_params = [app, params, {}]
        
        logger.info(f"Submitting job to app '{app}'")
        
        # Log full params at DEBUG level
        logger.debug(
            f"Job submission details:\n"
            f"  Method: {method}\n"
            f"  App: {app}\n"
            f"  Params: {json.dumps(params, indent=2)}\n"
            f"  Auth token present: {bool(self.auth_token)}"
        )
        
        # Make JSON-RPC call
        result = self.call(method, rpc_params)
        
        # Log the actual result at DEBUG level
        logger.debug(
            f"Result from {method}:\n"
            f"  Type: {type(result)}\n"
            f"  Value: {json.dumps(result, indent=2) if isinstance(result, (dict, list)) else result}"
        )
        
        # Handle response format: BV-BRC returns a list with one dict
        task_info = None
        if isinstance(result, list):
            if len(result) == 0:
                raise ValueError(
                    f"Response from {method} is an empty list. Expected task information."
                )
            # Extract first element from list
            task_info = result[0]
            logger.debug(f"Extracted task info from list: result[0]")
        elif isinstance(result, dict):
            task_info = result
        else:
            raise ValueError(
                f"Unexpected result type from {method}: {type(result)}. "
                f"Expected list or dict. Result: {result}"
            )
        
        # Verify task_info is a dict
        if not isinstance(task_info, dict):
            raise ValueError(
                f"Expected dict task info from {method}, got {type(task_info)}. "
                f"Task info: {task_info}"
            )
        
        # Extract task_id - BV-BRC uses 'id' field, not 'task_id'
        task_id = task_info.get("id") or task_info.get("task_id")
        if not task_id:
            logger.error(
                f"Response from {method} missing 'id' or 'task_id' field:\n"
                f"  Task info keys: {list(task_info.keys())}\n"
                f"  Task info: {json.dumps(task_info, indent=2)}"
            )
            raise ValueError(
                f"Response from {method} missing 'id' or 'task_id' field. "
                f"Task info: {task_info}"
            )
        
        # Log successful submission summary
        logger.info(f"Job submitted successfully to '{app}': task_id={task_id}")
        
        # Log details at DEBUG level
        state_code = task_info.get('state_code', 'unknown')
        owner = task_info.get('owner', 'unknown')
        logger.debug(
            f"Job submission details:\n"
            f"  Task ID: {task_id}\n"
            f"  State: {state_code}\n"
            f"  Owner: {owner}\n"
            f"  Application: {task_info.get('application_id', app)}"
        )
        
        return str(task_id)

