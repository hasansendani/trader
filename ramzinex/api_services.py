from aiohttp import ClientSession

BASE_URL = "https://publicapi.ramzinex.com/exchange/api/v1.0/exchange/"


async def get_data(url):
   async with ClientSession() as session:
       async with session.get(url) as resp:
           data = await resp.json()
           if resp.status == 200 and 'data' in data:
               return data['data']

async def get_last_trades(pair_id):
   url = BASE_URL + "orderbooks/{}/trades".format(pair_id)
   return await get_data(url)

async def get_symbols():
   url = BASE_URL + 'pairs'
   return await get_data(url)   



# trade_object = {
#             'pair': pair_name,
#             'price': trade[0],
#             'amount': trade[1],
#             'total': trade[0] * trade[1],
#             'side': trade[3],
#             'time': trade[4],
#             'unifier': trade[5]
#         }

