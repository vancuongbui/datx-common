import httpx
from tenacity import retry, stop_after_attempt, wait_fixed
from common.utils.httpx_helper import handle_httpx_error
from common.market_data.binance.config import DEFAULT_HEADERS, BINANCE_URL


class BinanceClient:
    def __init__(self, timeout: float = 30) -> None:
        self.client = httpx.Client(timeout=timeout, headers=DEFAULT_HEADERS)

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(1), reraise=True)
    @handle_httpx_error(reraise=True)
    def get_fiat_payment_quote(
        self,
        base_currency: str,
        crypto_currency: str,
        fiat_currency: str,
        requested_amount: int = 1,
        business_type: str = "BUY",
    ):
        data = {
            "baseCurrency": base_currency,
            "cryptoCurrency": crypto_currency,
            "fiatCurrency": fiat_currency,
            "requestedAmount": str(requested_amount),
            "businessType": business_type,
            "paymentMethodCodeList": [],
            "p2pChannelCodeList": [],
        }
        response = self.client.post(BINANCE_URL.FIAT_PAYMENT_QUOTE, json=data)
        response.raise_for_status()
        json_data = response.json()
        return json_data["data"]
