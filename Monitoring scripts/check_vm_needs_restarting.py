#!/usr/bin/python3
import os
from datetime import datetime as dt
from time import strftime
import datetime
import re

def nowTime():
    nowstrt = datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S")
    return nowstrt

list_1 =[]
list_2 = []

vms = [
	{
		"host": "msk00-mrx01",
		"user": "murex",
	},
	{
		"host": "vs-weu01-mrx02",
		"user": "murex",
	},
	{
		"host": "vs-weu01-mrx03",
		"user": "murex",
	},
	{
		"host": "vs-weu01-mrx04",
		"user": "murex",
	},
        {
                "host": "vs-weu01-mrx06",
                "user": "murex",
        },
	{
		"host": "vs-weu01-mrz01",
		"user": "murex",
	},
	{
		"host": "vs-weu01-syb01",
		"user": "sybase",
	},
	{
		"host": "vs-weu02-syb01",
		"user": "sybase",
	},
]

for vm in vms:
	check = os.popen(f"""ssh {vm["user"]}@{vm["host"]} 'needs-restarting -r'""").read().strip()

	result = vm["host"]
	if ("No core libraries or services have been updated." in check):
		result = f"{result} : No"
	else:
		result = f"{result} : Yes"
	list_1.append(result)

for item in list_1:
	body = "\n" . join(str(item) for item in list_1[0:100])

if "Yes" in body:
	list_2.append(item)
	print('CRITICAL', '\nNeed restarting servers:', '\n' + body + '\n')
	print('Comment: Restarting servers should be on weekends. Need to get approve from Petr Pokushalov.')
else:
	print('OK\n')

print("\nTIMESTAMP:\n" + nowTime())
print("Script: /mrx/share/scripts/check_vm_needs_restarting.py\n")
