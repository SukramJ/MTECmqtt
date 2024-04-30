""" 
Read YAML config files
(c) 2024 by Christian Rödel 
"""
import yaml
import os
import sys
import logging
import socket

_LOGGER = logging.getLogger(__name__)

#----------------------------------------
# Create new config file
def create_config_file():
  print("Creating config.yaml")

  # Resolve hostname
  try:
    ip_addr=socket.gethostbyname('espressif')
    print(f"Found espressif server: {ip_addr}")
  except socket.error:
    print("Couldn't find espressif server")
    ip_addr=input("Please enter IP address of espressif server: ")

  opt=input("Enable HomeAssistant support? (y/N): ")
  if opt.lower()=='y':
    hass_cfg="HASS_ENABLE : True"
  else:    
    hass_cfg="HASS_ENABLE : False"

  # Read template 
  try:
    BASE_DIR = os.path.dirname(__file__) # Base installation directory
    templ_fname = os.path.join(BASE_DIR, "config-template.yaml")
    with open(templ_fname, "r") as file: 
      data = file.read()   
  except Exception as ex:
    print(f"ERROR - Couldn't read 'config-template.yaml': {ex}")
    return False

  # Customize
  data = data.replace('HASS_ENABLE : False', hass_cfg) 
  data = data.replace('MODBUS_IP : espressif', 'MODBUS_IP : "' + ip_addr +'"') 

  # Write customized config
  cfg_path = os.environ.get('XDG_CONFIG_HOME') or os.environ.get('APPDATA')
  if cfg_path: # Usually something like ~/.config/mtecmqtt/config.yaml resp. 'C:\\Users\\xxxx\\AppData\\Roaming'
    cfg_fname = os.path.join(cfg_path, "mtecmqtt", "config.yaml")  
  else:
    cfg_fname = os.path.join(os.path.expanduser("~"), ".config", "mtecmqtt", "config.yaml")  # ~/.config/mtecmqtt/config.yaml

  try:
    os.makedirs(os.path.dirname(cfg_fname), exist_ok=True)
    with open(cfg_fname, "w") as file: 
      file.write(data) 
  except Exception as ex:
    _LOGGER.error("ERROR - Couldn't write %s: %s", cfg_fname, ex)
    return False

  _LOGGER.info("Successfully created %s", cfg_fname)
  return True

# Read configuration from YAML file
def init_config():
  # Look in different locations for config.yaml file
  conf_files = []
  conf_files.append(os.path.join(os.getcwd(), "config.yaml"))  # CWD/config.yaml
  cfg_path = os.environ.get('XDG_CONFIG_HOME') or os.environ.get('APPDATA')
  if cfg_path: # Usually something like ~/.config/mtecmqtt/config.yaml resp. 'C:\\Users\\xxxx\\AppData\\Roaming'
    conf_files.append(os.path.join(cfg_path, "mtecmqtt", "config.yaml"))  
  else:
    conf_files.append(os.path.join(os.path.expanduser("~"), ".config", "mtecmqtt", "config.yaml"))  # ~/.config/mtecmqtt/config.yaml
  
  cfg = False
  for fname_conf in conf_files:
    try:
      with open(fname_conf, 'r', encoding='utf-8') as f_conf:
        cfg = yaml.safe_load(f_conf)
        _LOGGER.info("Using config YAML file: %s", fname_conf)
        break
    except IOError as err:
      _LOGGER.debug("Couldn't open config YAML file: %s", str(err))
    except yaml.YAMLError as err:
      _LOGGER.debug("Couldn't read config YAML file %s: %s", fname_conf, str(err))

  return cfg  

#----------------------------------------
# Read inverter registers and their mapping from YAML file
def init_register_map():
  BASE_DIR = os.path.dirname(__file__) # Base installation directory
  try:
    fname_regs = os.path.join(BASE_DIR, "registers.yaml")
    with open(fname_regs, 'r', encoding='utf-8') as f_regs:
      r_map = yaml.safe_load(f_regs)
  except IOError as err:
    _LOGGER.fatal("Couldn't open registers YAML file: %s", str(err))
    sys.exit(1)
  except yaml.YAMLError as err:
    _LOGGER.fatal("Couldn't read config YAML file %s: %s", fname_regs, str(err))
    sys.exit(1)
    
  # Syntax checks 
  register_map = {}
  p_mandatory = [
    "name", 
  ]
  p_optional = [ 
    # param, default
    [ "length", None ], 
    [ "type", None ],
    [ "unit", "" ],
    [ "scale", 1 ],
    [ "writable", False ],
    [ "mqtt", None ],
    [ "group", None ],
  ] 
  register_groups = []

  for key, val in r_map.items():
    # Check for mandatory paramaters
    for p in p_mandatory: 
      error = False
      if not val.get(p):
        _LOGGER.warning("Skipping invalid register config: %s. Missing mandatory parameter: %s.",  key, p )
        error = True
        break

    if not error: # All madatory parameters found   
      item = val.copy()  
      # Check optional parameters and add defaults, if not found
      for p in p_optional:  
        if not item.get(p[0]):
          item[p[0]] = p[1]
      register_map[key] = item # Append to register_map
      if item["group"] and item["group"] not in register_groups:
        register_groups.append(item["group"]) # Append to group list
  return register_map, register_groups

#----------------------------------------
logging.basicConfig( level=logging.INFO, format="[%(levelname)s] %(filename)s: %(message)s" )
cfg = init_config()
if not cfg:
  if create_config_file():  # Create a new config
    cfg = init_config()
    if not cfg:
      _LOGGER.fatal("Couldn't open config YAML file")
      sys.exit(1)
  else:
    _LOGGER.fatal("Couldn't create config YAML file")
    sys.exit(1)

register_map, register_groups = init_register_map()

#--------------------------------------
# Test code only
if __name__ == "__main__":
  _LOGGER.info( "Config: %s",  str(cfg))
  _LOGGER.info( "Register_map: %s",  str(register_map))
  