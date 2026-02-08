"""
RSA signing utilities for Walmart API authentication.

Implements the signature generation required by Walmart's API authentication
scheme. Headers are canonicalized (sorted and formatted consistently) before
being signed with an RSA private key using PKCS#1 v1.5 padding and SHA-256.

See: https://developer.walmart.com/doc/us/us-mp/us-mp-auth/
"""

import base64
from typing import Dict, Tuple

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey


class WalmartSigner:
    """
    Utility class for signing Walmart API requests.
    
    Provides methods for header canonicalization, private key loading,
    and RSA signature generation following Walmart's authentication spec.
    """

    @staticmethod
    def canonicalize(headers: Dict[str, str]) -> Tuple[str, str]:
        """
        Canonicalize headers for consistent signing.
        
        Walmart requires headers to be sorted alphabetically and formatted
        in a specific way before signing. This ensures both client and server
        produce identical signatures for verification.
        
        Args:
            headers: Dictionary of header key-value pairs to canonicalize
        
        Returns:
            Tuple of (header_names, header_values) where:
                - header_names: Semicolon-separated sorted header names
                - header_values: Newline-separated sorted header values
        
        Example:
            >>> canonicalize({"WM_CONSUMER.ID": "abc", "WM_SEC.KEY_VERSION": "1"})
            ('WM_CONSUMER.ID;WM_SEC.KEY_VERSION;', 'abc\n1\n')
        """
        names = []
        values = []

        # Sort headers alphabetically for consistent ordering
        for key in sorted(headers.keys()):
            names.append(f"{key.strip()};")
            values.append(f"{headers[key].strip()}\n")

        return "".join(names), "".join(values)

    @staticmethod
    def load_private_key(path: str) -> RSAPrivateKey:
        """
        Load an RSA private key from file.
        
        Supports multiple key formats:
        - PEM format (PKCS#1 or PKCS#8) - begins with "-----BEGIN"
        - Raw base64-encoded DER format (PKCS#8)
        
        Args:
            path: Path to private key file
        
        Returns:
            Loaded RSA private key object
        
        Raises:
            ValueError: If key format is invalid or unsupported
            FileNotFoundError: If key file doesn't exist
        """
        with open(path, "rb") as f:
            data = f.read()

        # PEM format (PKCS#1 or PKCS#8)
        if b"BEGIN" in data:
            return serialization.load_pem_private_key(
                data,
                password=None,
            )

        # Raw base64-encoded DER (PKCS#8)
        der = base64.b64decode(data)
        return serialization.load_der_private_key(
            der,
            password=None,
        )

    @staticmethod
    def sign(private_key: RSAPrivateKey, payload: str) -> str:
        """
        Sign a payload using RSA-SHA256 with PKCS#1 v1.5 padding.
        
        This is the signature algorithm required by Walmart's API. The
        payload (canonicalized header values) is signed and returned as
        a base64-encoded string.
        
        Args:
            private_key: RSA private key for signing
            payload: String to sign (typically canonicalized header values)
        
        Returns:
            Base64-encoded signature string
        
        Example:
            >>> key = load_private_key("path/to/key.pem")
            >>> signature = sign(key, "canonical_header_values")
            >>> # signature is base64-encoded, ready for WM_SEC.AUTH_SIGNATURE header
        """
        signature = private_key.sign(
            payload.encode("utf-8"),
            padding.PKCS1v15(),
            hashes.SHA256(),
        )
        return base64.b64encode(signature).decode("utf-8")