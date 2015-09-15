import os, errno, codecs

def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc: # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else: raise

def read_config(config_file, params):
    with codecs.open(config_file, encoding='utf-8', mode='r') as fconfig:
        line_idx = 0
        for line in fconfig:
            line_idx += 1
            line_strip = line.strip()
            if len(line_strip) > 0 and line_strip[0] != '#':
                comment_pos = line_strip.find('#')
                if comment_pos > 0:
                    line_strip = line_strip[0:comment_pos]
                param_val = line_strip.split('=')
                assert len(param_val) == 2, 'Parameters in line %d does not take the form of "param = value"' %line_idx
                param, val = param_val
                param = param.strip()
                param = param.strip('\'"')
                val = val.strip()
                val = val.strip('\'"')
                assert len(param) > 0 and len(val) > 0, 'Parameters in line %d does not take the form of "param = value"'%line_idx
                params[param] = val
