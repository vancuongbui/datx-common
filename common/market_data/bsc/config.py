class BSC_URL:
    CATEGORY = "https://tradeapi.bsc.com.vn/userdata/recommendation/symbolsByCategory"
    TRADING_VIEW = "https://priceapi.bsc.com.vn/tvchart/history"


BSC_CATEGORY_ID_MAPPING = {
    "BSC10": "1",
    "BSC30": "2",
    "BSC50": "3",
}

BSC_TRADING_VIEW_RESOLUTION_MAPPING = {"day": "1D"}

BSC_TRADING_VIEW_SPECIAL_SYMBOL_MAPPING = {"VNINDEX": "HOSE", "VN30": "30", "HNXINDEX": "HNX", "UPCOMINDEX": "UPCOM"}
