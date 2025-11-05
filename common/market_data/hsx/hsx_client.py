import httpx
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_fixed
import datetime
from bs4 import BeautifulSoup
from common.market_data.hsx.config import HSX_URL, HSX_DEFAULT_HEADERS
from common.utils.time import rand_delay
from common.utils.httpx_helper import handle_httpx_error
import pytz


class HsxClient:
    def __init__(self, timeout=30) -> None:
        self.client = httpx.Client(follow_redirects=True, headers=HSX_DEFAULT_HEADERS, timeout=timeout)
        self.local_tz = pytz.FixedOffset(420)  # UTC+7

    @retry(stop=stop_after_attempt(5), wait=wait_fixed(5), reraise=True)
    @handle_httpx_error(reraise=True)
    @rand_delay(1, 5)
    def _get_data(self, url: str, headers: dict, params: dict):
        res = self.client.get(url, headers=headers, params=params)
        res.raise_for_status()
        json_data = res.json()
        return json_data["rows"]

    def _parse_vietnamese_datetime(self, datetime_str: str):
        datetime_str = datetime_str.replace("SA", "AM").replace("CH", "PM")
        format_str = "%d/%m/%Y %I:%M:%S %p"
        return datetime.datetime.strptime(datetime_str, format_str).replace(tzinfo=self.local_tz)

    def _parse_exchange_activity_news(self, json_data: dict):
        new_id = json_data["id"]
        [_, date_str, html_content] = json_data["cell"]
        date = self._parse_vietnamese_datetime(date_str)
        soup = BeautifulSoup(html_content, "html.parser")
        a_tag = soup.find("a")
        url = f"https://www.hsx.vn{a_tag['href']}"
        title = a_tag.text
        result = {"id": new_id, "publish_date": date, "title": title, "detail_page_url": url}
        return result

    def get_exchange_activity_news(
        self, from_date: datetime.date = datetime.date.today(), to_date: datetime.date = datetime.date.today()
    ):
        url = HSX_URL.EXCHANGE_ACTIVITY_NEWS
        page = 1
        params = {
            "exclude": "00000000-0000-0000-0000-000000000000",
            "lim": "True",
            "pageFieldName1": "FromDate",
            "pageFieldValue1": from_date.strftime("%d.%m.%Y"),
            "pageFieldOperator1": "eq",
            "pageFieldName2": "ToDate",
            "pageFieldValue2": to_date.strftime("%d.%m.%Y"),
            "pageFieldOperator2": "eq",
            "pageFieldName3": "TokenCode",
            "pageFieldValue3": "",
            "pageFieldOperator3": "eq",
            "pageFieldName4": "CategoryId",
            "pageFieldValue4": "822d8a8c-fd19-4358-9fc9-d0b27a666611",
            "pageFieldOperator4": "eq",
            "pageCriteriaLength": "4",
            "_search": "false",
            "nd": "1720061152155",
            "rows": "30",
            "page": page,
            "sidx": "id",
            "sord": "desc",
        }
        headers = {
            "Referer": "https://www.hsx.vn/Modules/Cms/Web/NewsByCat/822d8a8c-fd19-4358-9fc9-d0b27a666611?fid=0318d64750264e31b5d57c619ed6b338"
        }

        data = []
        while True:
            params["page"] = page
            rows = self._get_data(url, headers, params)
            if not rows:
                break
            for row in rows:
                data.append(self._parse_exchange_activity_news(row))
            page += 1
        return pd.DataFrame(data)

    def _parse_attachment(self, json_data: dict):
        attachment_id = json_data["id"]
        attachment_name = json_data["cell"][1]
        download_url = f"{HSX_URL.ATTACHMENT_DOWNLOAD}?id={attachment_id}"
        result = {"id": attachment_id, "attachment_name": attachment_name, "download_url": download_url}
        return result

    def get_attachments(self, article_ids: list[str]):
        params = {
            "_search": "false",
            "rows": "30",
            "page": "1",
            "sidx": "id",
            "sord": "desc",
        }
        data = []
        for article_id in article_ids:
            url = f"{HSX_URL.ATTACHMENT_METADATA}/{article_id}"
            headers = {"Referer": f"https://www.hsx.vn/Modules/Cms/Web/ViewArticle/{article_id}"}
            rows = self._get_data(url, headers, params)
            for row in rows:
                record = {"article_id": article_id, **self._parse_attachment(row)}
                data.append(record)
        return pd.DataFrame(data)
