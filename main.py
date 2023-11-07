import datetime
import pymongo
import bson

import asyncio
from aiogram import Bot, Dispatcher, types
from config import BOT_TOKEN

import re
import json
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

client = pymongo.MongoClient("mongodb://localhost:27017/")

with open("sampleDB\sample_collection.bson", "rb") as file:
    src_data = bson.decode_all(file.read())
src_db = client["src_data_db"]
collection = src_db["mycollection"]

res = {}
def get_sums():
    dt_from = convert_input_data("dt_from")
    dt_upto = convert_input_data("dt_upto")
    group_type = input_data["group_type"]
    dt_start = dt_from
    dt_end = dt_start
    res["dataset"]=[]
    res["labels"]=[]

    while dt_end < dt_upto:
        dt_end = increment_month(dt_start, group_type)
        query = {'dt': {'$gte': dt_start, '$lt': dt_end}}
        print(dt_start, dt_end)
        res["dataset"].append(0)
        res["labels"].append(dt_start.strftime("%Y-%m-%dT%H:%M:%S"))
        for i in collection.find(query, {"_id":0, "value":1, "dt":1}):
            res["dataset"][-1] = res["dataset"][-1] + i["value"]
        dt_start = increment_month(dt_start, group_type)
    crutch(dt_upto)

def convert_input_data(src_dt):
    return datetime.datetime.strptime(input_data[src_dt], "%Y-%m-%dT%H:%M:%S")

def increment_month(datetime_obj, group_type):
    if group_type == "month":
        if datetime_obj.month == 12:
            datetime_obj = datetime_obj.replace(year=datetime_obj.year+1, month=1)
            return datetime_obj
        else:
            datetime_obj = datetime_obj.replace(month=datetime_obj.month+1)
            return datetime_obj
    if group_type == "day":
        datetime_obj = datetime_obj + datetime.timedelta(days=1)
        return datetime_obj
    if group_type == "hour":
        datetime_obj = datetime_obj + datetime.timedelta(hours=1)
        return datetime_obj

def crutch(date):
    if date.minute == 0:
        res["dataset"].append(0)
        res["labels"].append(date.strftime("%Y-%m-%dT%H:%M:%S"))

@dp.message()
async def answer(message: types.Message):
    if re.match(r'{.+}', re.sub("\n", "", message.text)):
        global input_data
        input_data = json.loads(message.text)
        get_sums()
        await message.answer(re.sub("'","\"",str(res)))
    else:
        await message.answer('''Невалидный запос. Пример запроса:
{"dt_from": "2022-09-01T00:00:00", "dt_upto": "2022-12-31T23:59:00", "group_type": "month"}''')
async def main():
    global input_data
    await dp.start_polling(bot)
    await answer()

if __name__ == '__main__':
    asyncio.run(main())

