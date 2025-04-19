import asyncio
import os
import gzip
import json
from pathlib import Path

import httpx
import polars as pl
from dotenv import load_dotenv
from pydoll.browser.options import Options
from pydoll.browser.page import Page
from pydoll.browser.chrome import Chrome
from pydoll.constants import By

from pydoll_extension import human_element

load_dotenv()

url = 'https://lkdr.nalog.ru'
user_agent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"
headers = {
    "User-Agent": user_agent,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

async def get_auth_token(page: Page) -> str:
    result = await page.execute_script('localStorage["auth.token"]')
    return result['result']['result'].get('value')


async def auth(page: Page):
    phone_input = human_element(await page.find_element(
        By.CSS_SELECTOR,
        'input[type="tel"]',
    ))
    await phone_input.human_move()
    await phone_input.click()
    await phone_input.type_keys(os.environ['PHONE_NUM'])

    captcha = await page.wait_element(
        By.CSS_SELECTOR, 
        '#captcha-container',
        timeout=30,
        raise_exc=True
    )
    captcha = human_element(captcha)
    
    await captcha.human_move()
    await asyncio.sleep(0.1)
    await captcha.click()
    
    agreement_checkbox = human_element(await page.find_element(
        By.XPATH,
        "//span[contains(text(), 'Я согласен')]"
    ))
    await agreement_checkbox.human_move()
    await agreement_checkbox.click()

    
    submit_button = human_element(await page.find_element(
        By.XPATH,
        "//div[contains(text(), 'Отправить код')]"
    ))
    await submit_button.human_move()
    await submit_button.click()

    while True:
        if await get_auth_token(page):
            return
        await asyncio.sleep(1)


async def pull_receipts(page):
    has_more = True
    pagination_page = 0
    while has_more:
        r = httpx.post(
            f'{url}/api/v1/receipt',
            headers=headers | {'Authorization': f'Bearer {await get_auth_token(page)}'},
            cookies={
                cookie['name']: cookie['value']
                for cookie in await page.get_cookies()
            },
            json={
                "limit":10,
                "offset":pagination_page * 10,
                "dateFrom":None,
                "dateTo":None,
                "orderBy":"CREATED_DATE:DESC",
                "inn":None,"kktOwner":""
            }
        )
        
        data = r.json()
        with gzip.open('storage/receipt.jsonl.gz', 'at') as f:
            json.dump(data, f, ensure_ascii=False)
            f.write('\n')
        pagination_page += 1
        has_more = data.pop('hasMore')
        
        await asyncio.sleep(3)


async def pull_fiscal_data(page, receipts_task):
    while True:
        if not Path('storage/receipt.jsonl.gz').exists():
            await asyncio.sleep(1)
            continue
        else:
            pulled_receipts = pl.scan_ndjson('storage/receipt.jsonl.gz')

        if not Path('storage/fiscal_data.jsonl.gz').exists():
            pulled_fiscal_data = pl.DataFrame({'key': []}, schema={'key': pl.String}).lazy()
        else:
            pulled_fiscal_data = pl.scan_ndjson('storage/fiscal_data.jsonl.gz')

        to_fetch = (
            pulled_receipts.explode('receipts').unnest('receipts')
            .select('key')
            .join(pulled_fiscal_data, on='key', how='anti')
            .collect()['key']
        )

        for key in to_fetch:
            r = httpx.post(
                f'{url}/api/v1/receipt/fiscal_data',
                headers=headers | {'Authorization': f'Bearer {await get_auth_token(page)}'},
                cookies={
                    cookie['name']: cookie['value']
                    for cookie in await page.get_cookies()
                },
                json={'key': key}
            )
            data = r.json() | {'key': key}
            with gzip.open('storage/fiscal_data.jsonl.gz', 'at') as f:
                json.dump(data, f, ensure_ascii=False)
                f.write('\n')
            await asyncio.sleep(3)


        if receipts_task.done():
            return



async def main():
    browser_opts = Options()
    browser_opts.add_argument('--user-data-dir=chrome-profile')

    async with Chrome(options=browser_opts) as browser:
        await browser.start()
        page = await browser.get_page()
        await page.go_to(f'{url}/login')
        
        if not await get_auth_token(page):
            await auth(page)
       
        async with asyncio.TaskGroup() as tg:
            pull_receipts_task = tg.create_task(pull_receipts(page))
            tg.create_task(pull_fiscal_data(page, pull_receipts_task))
        

asyncio.run(main())
