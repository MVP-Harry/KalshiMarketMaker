import requests
import logging
import datetime
import base64
from typing import Dict, List, Optional
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.backends import default_backend
from cryptography.exceptions import InvalidSignature
import abc

class AbstractTradingAPI(abc.ABC):
    @abc.abstractmethod
    def get_price(self) -> float:
        pass

    @abc.abstractmethod
    def place_order(self, action: str, side: str, price: float, quantity: int, expiration_ts: int = None) -> str:
        pass

    @abc.abstractmethod
    def cancel_order(self, order_id: str) -> bool:
        pass

    @abc.abstractmethod
    def get_position(self) -> int:
        pass

    @abc.abstractmethod
    def get_orders(self) -> List[Dict]:
        pass

class KalshiTradingAPI(AbstractTradingAPI):
    """
    API client for Kalshi trading operations using API key authentication.
    Handles RSA-PSS signed requests, account management, and order execution.
    
    Follows Kalshi's official documentation:
    https://docs.kalshi.com/getting_started/api_keys
    """
    
    def __init__(self, market_ticker: str, api_key_id: str, private_key_path: str, base_url: str = 'https://api.elections.kalshi.com'):
        """
        Initialize Kalshi API client with API key authentication.
        
        Args:
            api_key_id: Your Kalshi API key ID
            private_key_path: Path to your private key .pem file
            base_url: Base URL (default: https://api.elections.kalshi.com)
        """
        self.api_key_id = api_key_id
        self.private_key_path = private_key_path
        self.market_ticker = market_ticker
        self.base_url = base_url.rstrip('/')  # Remove trailing slash if present
        self.private_key = None
        self.logger = self._setup_logger()
        
        # Load private key
        self._load_private_key()
    
    def _setup_logger(self) -> logging.Logger:
        """Set up logging."""
        logger = logging.getLogger('KalshiTradingAPI')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def _load_private_key(self):
        """Load private key from PEM file - follows Kalshi documentation."""
        try:
            with open(self.private_key_path, 'rb') as key_file:
                self.private_key = serialization.load_pem_private_key(
                    key_file.read(),
                    password=None,
                    backend=default_backend()
                )
            self.logger.info("Private key loaded successfully")
        except Exception as e:
            self.logger.error(f"Failed to load private key: {e}")
            raise
    
    def _sign_pss_text(self, text: str) -> str:
        """
        Sign text with private key using RSA-PSS.
        Follows Kalshi's sign_pss_text function exactly.
        
        Args:
            text: Text to sign (timestamp + method + path)
        
        Returns:
            Base64-encoded signature
        """
        message = text.encode('utf-8')
        try:
            signature = self.private_key.sign(
                message,
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.DIGEST_LENGTH
                ),
                hashes.SHA256()
            )
            return base64.b64encode(signature).decode('utf-8')
        except InvalidSignature as e:
            raise ValueError("RSA sign PSS failed") from e
    
    def _get_signed_headers(self, method: str, path: str) -> Dict[str, str]:
        """
        Generate signed headers for Kalshi API request.
        Follows Kalshi's documentation example exactly.
        
        Args:
            method: HTTP method (GET, POST, DELETE, etc.)
            path: Request path (must include /trade-api/v2/...)
        
        Returns:
            Dict of headers including signature
        """
        # Get current timestamp in milliseconds
        current_time = datetime.datetime.now()
        timestamp = current_time.timestamp()
        current_time_milliseconds = int(timestamp * 1000)
        timestamp_str = str(current_time_milliseconds)
        
        # Strip query parameters from path before signing
        path_without_query = path.split('?')[0]
        
        # Create message string: timestamp + method + path
        msg_string = timestamp_str + method + path_without_query
        
        # Sign the message
        sig = self._sign_pss_text(msg_string)
        
        # Return headers as per Kalshi documentation
        return {
            'KALSHI-ACCESS-KEY': self.api_key_id,
            'KALSHI-ACCESS-SIGNATURE': sig,
            'KALSHI-ACCESS-TIMESTAMP': timestamp_str
        }
    
    def get_balance(self) -> Dict:
        """
        Get current account balance.
        
        Returns:
            Dict with balance information
        """
        path = '/trade-api/v2/portfolio/balance'
        url = self.base_url + path
        
        try:
            headers = self._get_signed_headers('GET', path)
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            balance_data = response.json()
            
            balance = balance_data.get('balance', 0) # in cents
            
            self.logger.info(f"Current balance: ${balance:,.2f}")
            return {
                'balance': balance,
                'raw_data': balance_data
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get balance: {e}")
            return None

    def get_position(self) -> List[Dict]:
        """
        Get current open positions.
        
        Args:
            limit: Maximum number of positions to return
        
        Returns:
            List of position dictionaries
        """
        path = f'/trade-api/v2/portfolio/positions'
        url = self.base_url + path
        
        try:
            headers = self._get_signed_headers('GET', path)
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
            
            positions = data.get('market_positions', [])
            total_position = 0
            for position in positions:
                if position["ticker"] == self.market_ticker:
                    total_position += position["position"]

            self.logger.info(f"Found {len(positions)} open positions")
            self.logger.info(f"Current position: {total_position}")
            
            return total_position
            
        except Exception as e:
            self.logger.error(f"Failed to get positions: {e}")
            return []
    
    def get_price(self) -> Dict[str, float]:
        """
        Get orderbook for a specific market.
        """
        path = f'/trade-api/v2/markets/{self.market_ticker}/'
        url = self.base_url + path
        
        try:
            headers = self._get_signed_headers('GET', path)
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()

            yes_bid = float(data["market"]["yes_bid"]) / 100
            yes_ask = float(data["market"]["yes_ask"]) / 100
            no_bid = float(data["market"]["no_bid"]) / 100
            no_ask = float(data["market"]["no_ask"]) / 100
            
            yes_mid_price = round((yes_bid + yes_ask) / 2, 2)
            no_mid_price = round((no_bid + no_ask) / 2, 2)

            self.logger.info(f"Current yes mid-market price: ${yes_mid_price:.2f}")
            self.logger.info(f"Current no mid-market price: ${no_mid_price:.2f}")
            return {"yes": yes_mid_price, "no": no_mid_price}
            
        except Exception as e:
            self.logger.error(f"Failed to get orderbook for {ticker}: {e}")
            return None
        
    def place_order(self, 
                    action: str,
                    side: str, 
                    count: int,
                    price: float,
                    expiration_ts: int = None,
                    ) -> str:
        """
        Place an order on Kalshi.
        
        Args:
            ticker: Market ticker
            side: 'yes' or 'no'
            count: Number of contracts
            price: Price in cents (1-99) - required for limit orders
            order_type: 'limit' or 'market'
        
        Returns:
            Order response data
        """
        path = '/trade-api/v2/portfolio/orders'
        url = self.base_url + path
        
        payload = {
            'ticker': self.market_ticker,
            'action': action.lower(),
            'side': side,
            'count': count,
            'type': 'limit',
        }
        
        price_to_send = int(price * 100) # Cents
        if side == 'yes':
            payload['yes_price'] = price_to_send
        else:
            payload['no_price'] = price_to_send
        
        if expiration_ts is not None:
            payload['expiration_ts'] = expiration_ts
        
        try:
            self.logger.info(f"Placing order: {side.upper()} {count} contracts @ {price}¢ on {self.market_ticker}")
            
            headers = self._get_signed_headers('POST', path)
            response = requests.post(url, json=payload, headers=headers)
            response.raise_for_status()
            order_id = response.json()["order"]["order_id"]
            
            return str(order_id)
            
        except Exception as e:
            self.logger.error(f"Failed to place order: {e}")
            return None
    
    def cancel_order(self, order_id: str) -> bool:
        """
        Cancel an existing order.
        
        Args:
            order_id: Order ID to cancel
        
        Returns:
            True if successful
        """
        path = f'/trade-api/v2/portfolio/orders/{order_id}'
        url = self.base_url + path
        
        try:
            headers = self._get_signed_headers('DELETE', path)
            response = requests.delete(url, headers=headers)
            response.raise_for_status()
            
            self.logger.info(f"Order {order_id} cancelled successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to cancel order {order_id}: {e}")
            return False
        
    def get_orders(self) -> List[Dict]:
        path = '/trade-api/v2/portfolio/orders'
        url = self.base_url + path
        
        try:
            headers = self._get_signed_headers('GET', path)
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
            
            orders = data.get('orders', [])
            self.logger.info(f"Found {len(orders)} open orders")
            return orders
            
        except Exception as e:
            self.logger.error(f"Failed to get orders: {e}")
            return []


if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    api_key = os.getenv("KALSHI_API_KEY_ID")
    private_key_path = os.getenv("KALSHI_PRIVATE_KEY_PATH")
    
    # Initialize API client
    api = KalshiTradingAPI(api_key, private_key_path)
    
    # Test: Get balance
    print("Testing balance endpoint...")
    balance = api.get_balance()
    print(f"Balance: {balance}")

    # Test: Get position
    print("Testing position endpoint...")
    positions = api.get_positions()
    print(f"Positions: {positions}")

    EVENT_TICKER = "kxnflgame-26jan11bufjac".upper()
    markets = api.get_markets(EVENT_TICKER)
    tickers = [m["ticker"] for m in markets]

    for ticker in tickers:
        ob = api.get_orderbook(ticker)
        print(f"Orderbook for {ticker}: {ob}")