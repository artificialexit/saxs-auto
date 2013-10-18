from functools import wraps
from flask.ext.vbl import VBL
from flask.ext.beamline import Beamline

vbl = VBL()
beamline = Beamline()

def beamline_or_vbl(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not (vbl.current_user or beamline.current):
            return vbl.get_login_redirect()
        return f(*args, **kwargs)
    return decorated

