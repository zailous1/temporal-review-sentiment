"""
Walmart Product API client for fetching product reviews.

Implements Walmart's authentication scheme which requires request signing
using RSA private keys. See Walmart's API documentation:
https://developer.walmart.com/doc/us/us-mp/us-mp-auth/
"""

import time
from typing import Dict, Any, Optional

import aiohttp

from review_sentiment.walmart_client.signer import WalmartSigner

# API timeout for review fetching (seconds)
REQUEST_TIMEOUT_SECONDS = 10


class WalmartAPI:
    """
    Client for Walmart's Product Review API.
    
    Handles request signing and pagination for the reviews endpoint.
    Requires Walmart API credentials (consumer ID, key version, private key).
    """
    
    def __init__(
        self,
        consumer_id: str,
        key_version: str,
        private_key_path: str,
        api_base: str,
    ) -> None:
        """
        Initialize Walmart API client with authentication credentials.
        
        Args:
            consumer_id: Walmart API consumer ID
            key_version: Version number of the signing key
            private_key_path: Path to RSA private key file (PEM or DER format)
            api_base: Base URL for Walmart API (e.g., https://developer.api.walmart.com/...)
        """
        self.consumer_id = consumer_id
        self.key_version = key_version
        self.api_base = api_base.rstrip("/")
        self.signer = WalmartSigner()
        self.private_key = self.signer.load_private_key(private_key_path)

    def _signed_headers(self) -> Dict[str, str]:
        """
        Generate signed headers for Walmart API authentication.
        
        Creates a timestamp, canonicalizes headers, and signs them with
        the RSA private key. This signature proves the request originates
        from the registered API consumer.
        
        Returns:
            Dictionary of headers including authentication signature
        """
        # Current time in milliseconds (Walmart's required format)
        timestamp = str(int(time.time() * 1000))

        headers_to_sign = {
            "WM_CONSUMER.ID": self.consumer_id,
            "WM_CONSUMER.INTIMESTAMP": timestamp,
            "WM_SEC.KEY_VERSION": self.key_version,
        }

        # Canonicalize and sign headers
        _, canonicalized = self.signer.canonicalize(headers_to_sign)
        signature = self.signer.sign(self.private_key, canonicalized)

        headers_to_sign["WM_SEC.AUTH_SIGNATURE"] = signature
        return headers_to_sign

    async def get_reviews(
        self,
        product_id: str,
        query: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Fetch one page of reviews for a Walmart product.
        
        The Walmart API returns up to 10 reviews per page. For pagination,
        the response includes a 'nextPage' field with a query string that
        should be passed to subsequent calls.
        
        Args:
            product_id: Walmart product identifier (numeric string)
            query: Optional query string for pagination (from previous response's 'nextPage')
        
        Returns:
            Dictionary containing:
                - reviews: List of review objects
                - name: Product name
                - salePrice: Current sale price
                - nextPage: Query string for next page (if more reviews exist)
        
        Raises:
            aiohttp.ClientError: On network or HTTP errors
            asyncio.TimeoutError: If request exceeds timeout
        
        Example:
            >>> api = WalmartAPI(consumer_id, key_version, key_path, base_url)
            >>> page1 = await api.get_reviews("14977205582")
            >>> page2 = await api.get_reviews("14977205582", page1.get("nextPage"))
        """
        # Build URL with optional pagination query
        if query:
            url = f"{self.api_base}/reviews/{product_id}?{query}"
        else:
            url = f"{self.api_base}/reviews/{product_id}"

        headers = self._signed_headers()
        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)

        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, headers=headers) as resp:
                resp.raise_for_status()
                data = await resp.json()

        return data