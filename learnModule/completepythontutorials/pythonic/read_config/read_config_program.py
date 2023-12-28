import configparser

config = configparser.ConfigParser()

config['GATEWAY'] = {'reg_aut': 'FINMA',
                     'batch_type': 19}

config['forg.example'] = {}

config['forg.example']['user'] = 'sujeet'

config['top.server.example'] = {}
top_secrete = config['top.server.example']
top_secrete['Port'] = '50022'
config['DEFAULT']['ForwardX11'] = 'yes'

path = "C:\\Users\\sujee\\pydev\\pyspark_learn_project\\conf\\py_values.config"

with open("C:\\Users\\sujee\\pydev\\pyspark_learn_project\\conf\\default.conf", 'w') as configfile:
    config.write(configfile)

# reading it back

config = configparser.ConfigParser()

config.sections()

config.read(path)

print(config.sections())

print('forge.example' in config)

print(config['DEFAULT']['Compression'])

topsecret = config['topsecret.server.example']

print(topsecret['port'])

for key in config['forge.example']:
    print(key)

'''always call as string'''

print(topsecret.getboolean('ForwardX11'))

print(config.getboolean('forge.example', 'Compression'))

# Fall back value

print(topsecret.get('cifer', 'RSA-256'))

config.get('forge.example', 'monster', fallback='no such file')

print("=" * 10)

print(config['forge.example']['regulator'].split(","))
