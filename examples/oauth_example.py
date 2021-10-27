import time
import functools
from robot.libraries.BuiltIn import BuiltIn


def oauth_cb(oauth_config):
      BuiltIn().set_global_variable("${SEEN_RF_OAUTH_CB}", True)
      return 'token', time.time() + 300.0

def get_token(oauth_config):
      return functools.partial(oauth_cb, oauth_config)
