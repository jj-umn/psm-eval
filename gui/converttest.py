import yaml
f = open('settings_example.yaml')
# use safe_load instead load
print yaml.safe_load(f)
f.close()
