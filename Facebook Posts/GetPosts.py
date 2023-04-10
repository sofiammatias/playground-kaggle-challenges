# Will try to get info from posts from https://herbalcochete.comment
# Using BeautifulSoap


from urllib.request import urlopen
import urllib.parse
from bs4 import BeautifulSoup
import ssl, requests, re, datetime
# fix error with python 3.10 and BeautifulSoup: https://stackoverflow.com/questions/69515086/beautifulsoup-attributeerror-collections-has-no-attribute-callable
import collections
collections.Callable = collections.abc.Callable

# Ignore SSL certificate errors
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

# Get url to read
post = input ('Enter post URL:')
print ('Retrieving post',post)
try:
    html = urlopen(post, context=ctx).read()
    soup = BeautifulSoup(html, "html.parser")
except:
    html = requests.get(post)
    soup = BeautifulSoup(html.text, "html.parser")
# Retrieve feature image
for link in soup.find_all('img'):
    image = link.get('src')
    x = re.search('https://herbalcochete.com/wp-content/', image)
    if x:
        featureimage = image
        break
    elif x is None:
        featureimage = 'ai eu'
# Retrieve post description (1st paragraph) and post title
description = soup.p.string
title = soup.title

# Print retrieved stuff
#print ('FEATURE IMAGE:',featureimage)
#print ('TITLE:',title.contents[0])
#print ('URL:', post)
#print ('DESCRIPTION:', description)

# Facebook tokens and IDs
fb_user_ID = '10220076123203869'
fb_user_AT = 'EAAKfIm0ZCLC4BAGZBus0TsLrLWRYdaELsepHJffu8SLryUpjDexcElWzjdmykNtzBrHqgYosxa2tPTVQl9Me1Tifs5JVKxbuORCghAJbfaIrYMB0ssESZCYQshBJciZCbmc4w0128enntVY0R5ZAhfvsA6VVswBHcqfWKPxvHTqbj9jT57M5GJqxC0o2ZBu38ZD'
fb_page_AT = 'EAAKfIm0ZCLC4BANhKwwnqiZBXwhKJMW2lI4Uvp1ADiklYN6m3dqqHvV01ZAlyQDJRRZB0TnIbnZBYWMp8uZBzA6KHPLp08GPZBMpGseJmtVR3Js3briMV3PLukstwOAzUgFzg1GQw490bIZCBioomIcLY7dlBBq4hFNyBt0AEaEtX8ADV6AbRt0gD3byVE8y5YIZD'
fb_page_ID = '114450916777127'

# Create timestamp in the future
# days = input ('Enter number of days to schudule publishing (day0 = today): ')
# try:
#    days = int(days)
#except:
#    days = 0
#if days <= 0:
#    days = 0
#    print ('This post will be published in Facebook in 1 .', days)
#else:
#    print ('This post will be published in Facebook in', days,'days from today.')
#current_time = datetime.datetime.now()
# print (current_time)
#unix_timestamp = current_time.timestamp()
#unix_timestamp = int (unix_timestamp)
# print (unix_timestamp)
#unix_timestamp_future = unix_timestamp + (60) + days * 24 * 60 * 60  # 1 min * 60 seconds or days in seconds
# print (unix_timestamp_future)

#Connect to Facebook and post the url in 'post', with message and link
msg = title.contents[0] + '\n' + post + '\n' +'\n' + description
post_url = 'https://graph.facebook.com/{}/feed'.format(fb_page_ID)
print ('Posted on Facebook: \n',msg)
payload = {
'message': msg,
'link': post,
'access_token': fb_page_AT
}
r = requests.post(post_url, data=payload)
print(r.text)


# Connect to Facebook
#mysock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#fb_user_ID = '10220076123203869'
#fb_user_AT = 'EAAKfIm0ZCLC4BAGZBus0TsLrLWRYdaELsepHJffu8SLryUpjDexcElWzjdmykNtzBrHqgYosxa2tPTVQl9Me1Tifs5JVKxbuORCghAJbfaIrYMB0ssESZCYQshBJciZCbmc4w0128enntVY0R5ZAhfvsA6VVswBHcqfWKPxvHTqbj9jT57M5GJqxC0o2ZBu38ZD'
#fb_page_AT = 'EAAKfIm0ZCLC4BANhKwwnqiZBXwhKJMW2lI4Uvp1ADiklYN6m3dqqHvV01ZAlyQDJRRZB0TnIbnZBYWMp8uZBzA6KHPLp08GPZBMpGseJmtVR3Js3briMV3PLukstwOAzUgFzg1GQw490bIZCBioomIcLY7dlBBq4hFNyBt0AEaEtX8ADV6AbRt0gD3byVE8y5YIZD'
#fb_page_ID = '114450916777127'
#fb_url = 'graph.facebook.com'
#url = 'https://' + fb_url + '/v14.0/' + fb_page_ID + '/feed?message=Hello%20Fans!&access_token=' + fb_page_AT
#print ('FB URL:', url)

#mysock.connect((fb_url, 9090))
#cmd = 'POST \ '+ url
#cmd = cmd.encode()
#print ('Command to send:',cmd)
#mysock.send(cmd)
#print ('Command sent!')

#while True:
#    data = mysock.recv(1024)
#    if len(data) < 1:
#        break
#    print(data.decode(),end='')

#mysock.close()
