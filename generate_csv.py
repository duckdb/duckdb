import csv
import random
import string

data_size = 100000000

def get_random_string(length=6):
    # choose from all lowercase letter
    letters = string.ascii_lowercase
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str

def generate_integers():
	my_list = []
	for x in range (data_size):
		my_list.append(str(random.randint(0,data_size)))
	with open('integers.csv','w') as f:
	  f.write('\n'.join(my_list))

def generate_integers_null():
	my_list = []
	for x in range (data_size):
		if x%10 == 0:
			my_list.append(str("NULL"))
		else:
			my_list.append(str(random.randint(0,data_size)))
	with open('integers_null.csv','w') as f:
	  f.write('\n'.join(my_list))

def generate_strings():
	my_list = []
	for x in range (data_size):
		my_list.append(get_random_string())
	with open('strings.csv','w') as f:
	  f.write('\n'.join(my_list))

def generate_strings_null():
	my_list = []
	for x in range (data_size):
		if x%10 == 0:
			my_list.append(str("NULL"))
		else:
			my_list.append(str(get_random_string()))
	with open('strings_null.csv','w') as f:
	  f.write('\n'.join(my_list))

generate_integers()
generate_integers_null()
generate_strings()
generate_strings_null()