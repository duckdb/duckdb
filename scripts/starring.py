import json, os, sys, glob, mimetypes, urllib.request, re, time
from bs4 import BeautifulSoup
import duckdb
con = duckdb.connect('github-starring.duckdb')
con.execute('create table if not exists ghuser (name string, company string)')
con.execute('create table if not exists last_next (next string)')

def do_req(url):

	req = urllib.request.Request(url)
	next_link = None
	resp = urllib.request.urlopen(req)
	raw_resp = resp.read().decode()
	soup = BeautifulSoup(raw_resp, 'html.parser')
	for ele in soup.find_all('div', {'class':'ml-3'}):
		org_ele = ele.find('svg', class_='octicon-organization')
		if org_ele is None:
			continue
		user =ele.find('a', {'data-hovercard-type': 'user'}).string 
		company = org_ele.parent.find('span').string.replace('@','').lower()
		con.execute('insert into ghuser values (?, ?)', [user, company])

		print("%s\t%s" % (user, company))

	time.sleep(1)
	next = soup.find('a', attrs={'rel':'nofollow'}, string='Next')
	if (next is not None):
		con.execute("update last_next set next=\'%s\'" % next['href'])
		do_req(next['href'])

	# con.execute('select * from ghuser order by name')
	# print(con.df())

next = con.execute('select * from last_next').fetchone()
print(next)
if next is None:
	next = 'https://github.com/duckdb/duckdb/stargazers'

print(next)

print(do_req(next))
