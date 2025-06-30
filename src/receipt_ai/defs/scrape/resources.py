import asyncio
from contextlib import asynccontextmanager
from typing import Any
from pathlib import Path

import dagster as dg
import httpx
from pydantic import PrivateAttr
from pydoll.browser import Chrome
from pydoll.browser.chromium.base import Browser
from pydoll.browser.options import ChromiumOptions
from pydoll.browser.tab import Tab
from pydoll.elements.web_element import WebElement
from typing_extensions import override


class TabResource(dg.ConfigurableResource[Tab]):
    """Dagster resource for managing Chrome browser instance."""
    
    base_url: str
    user_data_dir: str = "chrome-profile"
    phone_num: str = ""
    
    _browser: Browser = PrivateAttr()
    _tab: Tab = PrivateAttr()
    
    async def _get_auth_token(self, tab: Tab) -> str:
        """Extract authentication token from browser localStorage.
        
        Parameters
        ----------
        tab : Tab
            Browser tab instance
            
        Returns
        -------
        str
            Authentication token or empty string if not found
        """
        result = await tab.execute_script('localStorage["auth.token"]')
        return result.get('result', {}).get('result', {}).get('value', '')

    async def _authenticate(self, tab: Tab, phone_num: str) -> None:
        """Authenticate user via phone number and captcha.
        
        Parameters
        ----------
        tab : Tab
            Browser tab instance
        phone_num : str
            Phone number for authentication
        """
        phone_input = await tab.find(tag_name='input', type='tel') # pyright: ignore
        assert isinstance(phone_input, WebElement)

        await phone_input.click()
        await phone_input.type_text(phone_num)

        agreement_checkbox = await tab.find(
            tag_name='span',
            text='Я согласен',
        )
        assert isinstance(agreement_checkbox, WebElement)

        await agreement_checkbox.click()
        
        captcha = await tab.find(
            id='captcha-container',
            timeout=30,
            raise_exc=True
        )
        assert isinstance(captcha, WebElement)

        await asyncio.sleep(0.1)
        await captcha.click()
        
        submit_button = await tab.find(
            tag_name='div',
            text='Отправить код'
        )
        assert isinstance(submit_button, WebElement)

        await submit_button.click()

        # Wait for authentication to complete
        while True:
            if await self._get_auth_token(tab):
                return
            await asyncio.sleep(1)
    
    @asynccontextmanager
    async def yield_for_execution(self, context: dg.InitResourceContext): #pyright: ignore[reportIncompatibleMethodOverride]
        """Setup and teardown browser instance for the duration of execution.
        
        Parameters
        ----------
        context : dg.InitResourceContext
            Dagster initialization context
            
        Yields
        ------
        Tab
            The initialized and authenticated browser tab
        """
        context.log.info("Setting up Chrome browser instance") # pyright: ignore
       
        profile_path = Path(self.user_data_dir)
        if not profile_path.exists():
            profile_path.mkdir()

        options = ChromiumOptions()
        options.add_argument(f'--user-data-dir={self.user_data_dir}')

        # Setup: Create and initialize browser using async with
        async with Chrome(options=options) as browser:
            self._browser = browser
            self._tab = await browser.start()
            await self._tab.go_to(f'{self.base_url}/login')
            context.log.info("Browser instance initialized successfully") # pyright: ignore
            
            # Handle authentication
            
            # Check if already authenticated
            if not await self._get_auth_token(self._tab):
                context.log.info("Authenticating user") # pyright: ignore
                await self._authenticate(self._tab, self.phone_num)
                context.log.info("Authentication completed") # pyright: ignore
            else:
                context.log.info("Already authenticated") # pyright: ignore
            
            # Yield the tab directly for use during execution
            yield self._tab

    @override
    def create_resource(self, context: dg.InitResourceContext) -> Tab:
        """Create and return the Tab resource.
        
        Parameters
        ----------
        context : dg.InitResourceContext
            Dagster initialization context
            
        Returns
        -------
        Tab
            The initialized and authenticated browser tab
        """
        # This method is synchronous, but we need the async context manager
        # The actual resource creation happens in yield_for_execution
        if not hasattr(self, '_tab'):
            raise RuntimeError("Tab not initialized. This should not happen in normal execution.")
        return self._tab


class ApiResource(dg.ConfigurableResource): # pyright: ignore
    """Dagster resource for making API calls to the receipts service."""
    
    base_url: str
    default_headers: dict[str, str] = {
        'Content-Type': 'application/json',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    async def _get_auth_headers_and_cookies(self, tab: Tab) -> tuple[dict[str, str], dict[str, str]]:
        """Extract auth token and cookies from browser tab.
        
        Parameters
        ----------
        tab : Tab
            Browser tab instance
            
        Returns
        -------
        tuple[dict[str, str], dict[str, str]]
            Headers with auth token and cookies dictionary
        """
        # Get auth token from tab
        result = await tab.execute_script('localStorage["auth.token"]')
        auth_token = result.get('result', {}).get('result', {}).get('value', '')
        
        # Get cookies
        cookies = await tab.get_cookies()
        cookie_dict = {cookie['name']: cookie['value'] for cookie in cookies}
        
        # Prepare headers with auth
        headers = self.default_headers.copy()
        if auth_token:
            headers['Authorization'] = f'Bearer {auth_token}'
            
        return headers, cookie_dict
    
    async def fetch_receipts_page(
        self, 
        tab: Tab, 
        page_num: int, 
        limit: int = 10,
        date_from: str | None = None,
        date_to: str | None = None,
        order_by: str = "CREATED_DATE:DESC",
        inn: str | None = None,
        kkt_owner: str = ""
    ) -> dict[str, Any]:
        """Fetch a single page of receipts.
        
        Parameters
        ----------
        tab : Tab
            Browser tab instance
        page_num : int
            Page number to fetch
        limit : int, optional
            Number of receipts per page, by default 10
        date_from : str | None, optional
            Start date filter, by default None
        date_to : str | None, optional
            End date filter, by default None
        order_by : str, optional
            Sort order, by default "CREATED_DATE:DESC"
        inn : str | None, optional
            INN filter, by default None
        kkt_owner : str, optional
            KKT owner filter, by default ""
            
        Returns
        -------
        dict[str, Any]
            API response containing receipts data
        """
        headers, cookies = await self._get_auth_headers_and_cookies(tab)
        
        payload = {
            "limit": limit,
            "offset": page_num * limit,
            "dateFrom": date_from,
            "dateTo": date_to,
            "orderBy": order_by,
            "inn": inn,
            "kktOwner": kkt_owner
        }
        
        response = httpx.post(
            f'{self.base_url}/api/v1/receipt',
            headers=headers,
            cookies=cookies,
            json=payload,
            timeout=10.0
        )
        response.raise_for_status()
        return response.json()
    
    async def fetch_fiscal_data(self, tab: Tab, key: str) -> dict[str, Any]:
        """Fetch fiscal data for a specific receipt key.
        
        Parameters
        ----------
        tab : Tab
            Browser tab instance
        key : str
            Receipt key to fetch fiscal data for
            
        Returns
        -------
        dict[str, Any]
            Fiscal data response with added key field
        """
        headers, cookies = await self._get_auth_headers_and_cookies(tab)
        
        response = httpx.post(
            f'{self.base_url}/api/v1/receipt/fiscal_data',
            headers=headers,
            cookies=cookies,
            json={'key': key}
        )
        response.raise_for_status()
        return response.json() | {'key': key}
    
    async def fetch_receipt_details(self, tab: Tab, receipt_id: str) -> dict[str, Any]:
        """Fetch detailed information for a specific receipt.
        
        Parameters
        ----------
        tab : Tab
            Browser tab instance
        receipt_id : str
            Receipt ID to fetch details for
            
        Returns
        -------
        dict[str, Any]
            Detailed receipt information
        """
        headers, cookies = await self._get_auth_headers_and_cookies(tab)
        
        response = httpx.get(
            f'{self.base_url}/api/v1/receipt/{receipt_id}',
            headers=headers,
            cookies=cookies,
            timeout=10.0
        )
        response.raise_for_status()
        return response.json()
    
    async def search_receipts(
        self,
        tab: Tab,
        query: str,
        limit: int = 50,
        offset: int = 0
    ) -> dict[str, Any]:
        """Search receipts by query string.
        
        Parameters
        ----------
        tab : Tab
            Browser tab instance
        query : str
            Search query
        limit : int, optional
            Number of results to return, by default 50
        offset : int, optional
            Number of results to skip, by default 0
            
        Returns
        -------
        dict[str, Any]
            Search results
        """
        headers, cookies = await self._get_auth_headers_and_cookies(tab)
        
        payload = {
            "query": query,
            "limit": limit,
            "offset": offset
        }
        
        response = httpx.post(
            f'{self.base_url}/api/v1/receipt/search',
            headers=headers,
            cookies=cookies,
            json=payload
        )
        response.raise_for_status()
        return response.json()


@dg.definitions
def resources() -> dg.Definitions: # noqa: D103
    BASE_URL = 'https://lkdr.nalog.ru'
    phone_num = dg.EnvVar('PHONE_NUM').get_value()
    assert phone_num

    return dg.Definitions(
        resources={
            'tab': TabResource(
                base_url=BASE_URL,
                phone_num=phone_num
            ),
            'api': ApiResource(base_url=BASE_URL)
        }
    )
